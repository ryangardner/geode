/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * RegionVersionVector tracks the highest region-level version number of operations applied to a
 * region for each member that has the region.
 * <p>
 *
 */
public abstract class RegionVersionVector<T extends VersionSource<?>>
    implements DataSerializableFixedID, MembershipListener {

  private static final Logger logger = LogService.getLogger();

  // TODO:LOG:CONVERT: REMOVE THIS
  public static final boolean DEBUG =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "VersionVector.VERBOSE");



  //////////////////// The following statics exist for unit testing. ////////////////////////////

  /** maximum ms wait time while waiting for dominance to be achieved */
  public static final long MAX_DOMINANCE_WAIT_TIME =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "max-dominance-wait-time", 5000);

  /** maximum ms pause time while waiting for dominance to be achieved */
  public static final long DOMINANCE_PAUSE_TIME =
      Math.min(Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "dominance-pause-time", 300),
          MAX_DOMINANCE_WAIT_TIME);

  private static final int INITIAL_CAPACITY = 2;
  private static final int CONCURRENCY_LEVEL = 2;
  private static final float LOAD_FACTOR = 0.75f;
  ////////////////////////////////////////////////////////////////////////////////////////////////



  /** map of member to version h older. This is the actual version "vector" */
  private final ConcurrentHashMap<T, RegionVersionHolder<T>> memberToVersion;

  /** current version in the local region for generating next version */
  private final AtomicLong localVersion = new AtomicLong(0);
  /**
   * The list of exceptions for the local member. The version held in this RegionVersionHolder may
   * not be accurate, but the exception list is. We can have exceptions for our own id if we recover
   * from disk or GII from a peer that has exceptions from us.
   *
   * The version held in this object can lag behind the localVersion atomic long, because that long
   * is incremented without obtaining a lock. Operations that use the localException list are
   * responsible for updating the version of the local exceptions under lock.
   */
  private RegionVersionHolder<T> localExceptions;

  /** highest reaped tombstone region-version for this member */
  private final AtomicLong localGCVersion = new AtomicLong(0);

  /** the member that this version vector applies to */
  private T myId;



  /**
   * a flag stating whether this vector contains only the version information for a single member.
   * This is used when a member crashed to transmit only the version information for that member.
   */
  private boolean singleMember;

  /** a flag to prevent accidental serialization of a live member */
  private transient boolean isLiveVector;

  private transient LocalRegion region;

  private final ConcurrentHashMap<T, Long> memberToGCVersion;

  /** map of canonical IDs for this RVV that are not in the memberToVersion map */
  @SuppressWarnings("unchecked")
  private transient Map<T, T> canonicalIds = Collections.EMPTY_MAP;

  private final Object canonicalIdLock = new Object();

  /** is recording disabled? */
  private transient boolean recordingDisabled;

  /** is this a vector in a client cache? */
  private transient boolean clientVector;

  /**
   * this read/write lock is used to stop generation of new versions by the vector while a
   * region-level operation is underway. The locking scheme assumes that only one region-level RVV
   * operation is allowed at a time on a region across the distributed system. If that changes then
   * the locking scheme here may need additional work.
   */
  private final transient ReentrantReadWriteLock versionLock = new ReentrantReadWriteLock();
  private transient volatile boolean locked; // this is only modified by the version locking thread
  private transient volatile boolean doUnlock; // this is only modified by the version locking
                                               // thread
  private transient InternalDistributedMember lockOwner; // guarded by lockWaitSync

  private final transient Object clearLockSync = new Object(); // sync for coordinating thread
                                                               // startup and lockOwner setting

  /**
   * constructor used to create a cloned vector
   */
  protected RegionVersionVector(T ownerId, ConcurrentHashMap<T, RegionVersionHolder<T>> vector,
      long version, ConcurrentHashMap<T, Long> gcVersions, long gcVersion, boolean singleMember,
      RegionVersionHolder<T> localExceptions) {
    myId = ownerId;
    memberToVersion = vector;
    memberToGCVersion = gcVersions;
    localGCVersion.set(gcVersion);
    localVersion.set(version);
    this.singleMember = singleMember;
    this.localExceptions = localExceptions;
  }

  /**
   * deserialize a cloned vector
   */
  public RegionVersionVector() {
    memberToVersion = new ConcurrentHashMap<>(INITIAL_CAPACITY,
        LOAD_FACTOR, CONCURRENCY_LEVEL);
    memberToGCVersion =
        new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
  }

  /**
   * create a live version vector for a region
   */
  public RegionVersionVector(T ownerId) {
    this(ownerId, null);
  }

  /**
   * create a live version vector for a region
   */
  public RegionVersionVector(T ownerId, LocalRegion owner) {
    this(ownerId, owner, 0);
  }

  @VisibleForTesting
  RegionVersionVector(T ownerId, LocalRegion owner, long version) {
    myId = ownerId;
    isLiveVector = true;
    region = owner;
    localExceptions = new RegionVersionHolder<>(0);
    memberToVersion =
        new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
    memberToGCVersion =
        new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
    localVersion.set(version);
  }

  /**
   * Retrieve a vector that can be sent to another member. This clones all of the version
   * information to protect against concurrent modification during serialization
   */
  public RegionVersionVector<T> getCloneForTransmission() {
    Map<T, RegionVersionHolder<T>> liveHolders;
    liveHolders = new HashMap<>(memberToVersion);
    ConcurrentHashMap<T, RegionVersionHolder<T>> clonedHolders =
        new ConcurrentHashMap<>(liveHolders.size(), LOAD_FACTOR,
            CONCURRENCY_LEVEL);
    for (Map.Entry<T, RegionVersionHolder<T>> entry : liveHolders.entrySet()) {
      clonedHolders.put(entry.getKey(), entry.getValue().clone());
    }
    ConcurrentHashMap<T, Long> gcVersions = new ConcurrentHashMap<>(
        memberToGCVersion.size(), LOAD_FACTOR, CONCURRENCY_LEVEL);
    gcVersions.putAll(memberToGCVersion);
    RegionVersionHolder<T> clonedLocalHolder;
    clonedLocalHolder = localExceptions.clone();
    // Make sure the holder that we send to the peer does
    // have an accurate RegionVersionHolder for our local version
    return createCopy(myId, clonedHolders, localVersion.get(), gcVersions,
        localGCVersion.get(), false, clonedLocalHolder);
  }

  protected abstract RegionVersionVector<T> createCopy(T ownerId,
      ConcurrentHashMap<T, RegionVersionHolder<T>> vector, long version,
      ConcurrentHashMap<T, Long> gcVersions, long gcVersion, boolean singleMember,
      RegionVersionHolder<T> clonedLocalHolder);


  /**
   * Retrieve a vector that can be sent to another member. This clones only the version information
   * for the given ID.
   * <p>
   * The clone returned by this method does not have distributed garbage-collection information.
   */
  public RegionVersionVector<T> getCloneForTransmission(T mbr) {
    Map<T, RegionVersionHolder<T>> liveHolders;
    liveHolders = new HashMap<>(memberToVersion);
    RegionVersionHolder<T> holder = liveHolders.get(mbr);
    if (holder == null) {
      if (mbr.isDiskStoreId() && mbr.equals(myId)) {
        // For region recovered from disk, we may have local exceptions needs to be
        // brought back during region synchronization
        holder = localExceptions.clone();
        holder.setVersion(localVersion.get());
      } else {
        holder = new RegionVersionHolder<>(-1);
      }
    } else {
      holder = holder.clone();
    }
    return createCopy(myId,
        new ConcurrentHashMap<>(Collections.singletonMap(mbr, holder)), 0,
        new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL), 0, true,
        new RegionVersionHolder<>(-1));
  }


  /**
   * Retrieve a collection of tombstone GC region-versions
   */
  public Map<T, Long> getTombstoneGCVector() {
    Map<T, Long> result;
    synchronized (memberToGCVersion) {
      result = new HashMap<>(memberToGCVersion);
    }
    if (localGCVersion.get() != 0) {
      result.put(myId, localGCVersion.get());
    }
    return result;
  }


  /** returns true if all of the GC versions in the given map have already been processed here */
  public boolean containsTombstoneGCVersions(Map<T, Long> regionGCVersions) {
    Long myVersion = regionGCVersions.get(myId);
    if (myVersion != null) {
      if (localGCVersion.get() < myVersion) {
        return false;
      }
    }
    synchronized (memberToGCVersion) {
      for (Map.Entry<T, Long> entry : regionGCVersions.entrySet()) {
        Long version = memberToGCVersion.get(entry.getKey());
        if (version == null || version < entry.getValue()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * locks against new version generation and returns the current region version number
   *
   */
  public long lockForClear(String regionPath, DistributionManager dm,
      InternalDistributedMember locker) {
    lockVersionGeneration(regionPath, dm, locker);
    return localVersion.get();
  }

  /** unlocks version generation for clear() operations */
  public void unlockForClear(InternalDistributedMember locker) {
    synchronized (clearLockSync) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unlocking for clear, from member {} RVV {}", locker,
            System.identityHashCode(this));
      }
      if (lockOwner != null && !locker.equals(lockOwner)) {
        if (logger.isDebugEnabled()) {
          logger.debug("current clear lock owner was {} not unlocking", lockOwner);
        }
        // this method is invoked by memberDeparted events and may not be for the current lock owner
        return;
      }
      unlockVersionGeneration(locker);
    }
  }

  /**
   * This schedules a thread that owns the version-generation write-lock for this vector. The method
   * unlockVersionGeneration notifies the thread to release the lock and terminate its run.
   *
   * @param dm the distribution manager - used to obtain an executor to hold the thread
   * @param locker the member requesting the lock (currently not used)
   */
  private void lockVersionGeneration(final String regionPath, final DistributionManager dm,
      final InternalDistributedMember locker) {
    final CountDownLatch acquiredLock = new CountDownLatch(1);
    if (logger.isDebugEnabled()) {
      logger.debug("Locking version generation for {} region {} RVV {}", locker, regionPath,
          System.identityHashCode(this));
    }
    // this could block for a while if a limit has been set on the waiting-thread-pool
    dm.getExecutors().getWaitingThreadPool().execute(new Runnable() {
      @Override
      @edu.umd.cs.findbugs.annotations.SuppressWarnings(
          value = {"UL_UNRELEASED_LOCK", "IMSE_DONT_CATCH_IMSE"})
      public void run() {
        boolean haveLock = false;
        synchronized (clearLockSync) {
          try {
            // TODO Following code does not seem necessary as dlock has been taken
            // so no two threads will try to enter in this code section.
            while (locked && dm.isCurrentMember(locker)) {
              try {
                clearLockSync.wait();
              } catch (InterruptedException e) {
                // okay to ignore - release the lock and exit
              }
            }
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Waiting thread is now locking version generation for {} region {} RVV {}",
                  locker, regionPath, System.identityHashCode(this));
            }
            try {
              versionLock.writeLock().lock();
              lockOwner = locker;
              doUnlock = false;
              locked = true;
              haveLock = true;
              acquiredLock.countDown();
            } catch (IllegalMonitorStateException e) {
              // dlock on the clear() operation should prevent this from happening
              logger.fatal(
                  "Request from {} to block operations found that operations are already blocked by member {}.",
                  new Object[] {locker, lockOwner});
              return;
            }

            while (!doUnlock && dm.isCurrentMember(locker)) {
              try {
                clearLockSync.wait(250);
              } catch (InterruptedException e) {
                // okay to ignore - release the lock and exit
              }
            }
          } finally {
            if (haveLock) {
              locked = false; // this must be clear when the writeLock is released
                              // so we don't get warnings about it still being locked
              versionLock.writeLock().unlock();
              doUnlock = false;
              clearLockSync.notifyAll();
              // leave lockOwner set so we can see who the last lock request came from
            }
            acquiredLock.countDown();
          }
        }
      }
    });
    boolean interrupted = false;
    while (dm.isCurrentMember(locker)) {
      try {
        if (acquiredLock.await(250, TimeUnit.MILLISECONDS)) {
          break;
        }
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Done locking");
    }

  }

  private void unlockVersionGeneration(final InternalDistributedMember locker) {
    synchronized (clearLockSync) {
      doUnlock = true;
      clearLockSync.notifyAll();
    }
  }

  /**
   * return the next local version number
   */
  public long getNextVersion() {
    return getNextVersion(true);
  }

  /**
   * return the next local version number for a clear() operation, bypassing lock checks
   */
  public long getNextVersionWhileLocked() {
    return getNextVersion(false);
  }

  /**
   * return the next local version number
   */
  private long getNextVersion(boolean checkLocked) {
    if (checkLocked && locked) {
      // this should never be the case. If version generation is locked and we get here
      // then the path to this point is not protected by getting the version generation
      // lock from the RVV but it should be
      if (logger.isDebugEnabled()) {
        logger.debug("Generating a version tag when version generation is locked by {}",
            lockOwner);
      }
    }
    long new_version = localVersion.incrementAndGet();
    // since there could be special exception, we have to use recordVersion()
    recordVersion(getOwnerId(), new_version);
    return new_version;
  }

  /** obtain a lock to prevent concurrent clear() from happening */
  public void lockForCacheModification(LocalRegion owner) {
    if (owner.getServerProxy() == null) {
      versionLock.readLock().lock();
    }
  }

  /** release the lock preventing concurrent clear() from happening */
  public void releaseCacheModificationLock(LocalRegion owner) {
    if (owner.getServerProxy() == null) {
      versionLock.readLock().unlock();
    }
  }

  /** obtain a lock to prevent concurrent clear() from happening */
  public void lockForCacheModification() {
    versionLock.readLock().lock();
  }

  /** release the lock preventing concurrent clear() from happening */
  public void releaseCacheModificationLock() {
    versionLock.readLock().unlock();
  }

  private void syncLocalVersion() {
    long v = localVersion.get();
    synchronized (localExceptions) {
      if (v != localExceptions.version) {
        if (logger.isDebugEnabled()) {
          logger.debug("Adjust localExceptions.version {} to equal localVersion {}",
              localExceptions.version, localVersion.get());
        }
        localExceptions.version = v;
      }
    }
  }

  /**
   * return the current version for this member
   */
  public long getCurrentVersion() {
    synchronized (localExceptions) {
      syncLocalVersion();
      return localExceptions.getVersion();
    }
  }

  /**
   * return the current version for this member
   */
  public RegionVersionHolder<T> getLocalExceptions() {
    return localExceptions;
  }

  /**
   * return version holder for this member
   */
  public RegionVersionHolder<T> getHolderForMember(T id) {
    if (id.equals(myId)) {
      return localExceptions;
    } else {
      return memberToVersion.get(id);
    }
  }

  /**
   * returns the ID of the member that owns this version vector
   */
  public T getOwnerId() {
    return myId;
  }


  /**
   * turns off recording of versions for this vector. This can be used when recording of versions is
   * not necessary, as in an empty region
   */
  public void turnOffRecordingForEmptyRegion() {
    recordingDisabled = true;
  }

  /**
   * client version vectors only record GC numbers and don't keep exceptions, etc, because there
   * could be MANY of them
   */
  public void setIsClientVector() {
    clientVector = true;
  }

  /**
   * record all of the version information from an initial image provider
   */
  public void recordVersions(RegionVersionVector<T> otherVector) {
    synchronized (memberToVersion) {
      for (Map.Entry<T, RegionVersionHolder<T>> entry : otherVector.getMemberToVersion()
          .entrySet()) {
        T mbr = entry.getKey();
        RegionVersionHolder<T> otherHolder = entry.getValue();

        initializeVersionHolder(mbr, otherHolder);
      }
      // Get the set of local exceptions from the other vector
      // before directly accessing localExceptions, should sync its this.version with localVersion
      otherVector.syncLocalVersion();
      initializeVersionHolder(otherVector.getOwnerId(), otherVector.localExceptions);

      if (otherVector.getCurrentVersion() > 0
          && !memberToVersion.containsKey(otherVector.getOwnerId())) {
        recordVersion(otherVector.getOwnerId(), otherVector.getCurrentVersion());
      }

      // check if I have updates from members that the otherVector does not have
      // If yes, these are unfinished ops and should be cleaned
      for (T mbr : memberToVersion.keySet()) {
        if (!otherVector.memberToVersion.containsKey(mbr)
            && !mbr.equals(otherVector.getOwnerId())) {
          RegionVersionHolder holder = memberToVersion.get(mbr);
          initializeVersionHolder(mbr, new RegionVersionHolder(0));
        }
      }
      if (!otherVector.memberToVersion.containsKey(myId)
          && !myId.equals(otherVector.getOwnerId())) {
        initializeVersionHolder(myId, new RegionVersionHolder(0));
      }

      synchronized (memberToGCVersion) {
        for (Map.Entry<T, Long> entry : otherVector.getMemberToGCVersion().entrySet()) {
          T member = entry.getKey();
          Long value = entry.getValue();
          if (member.equals(myId)) {
            // If this entry is for our id, update our local GC version
            long currentValue;
            while ((currentValue = localGCVersion.get()) < value) {
              localGCVersion.compareAndSet(currentValue, value);
            }
          } else {
            // Update the memberToGCVersionMap.
            Long myVersion = memberToGCVersion.get(entry.getKey());
            if (myVersion == null || myVersion < entry.getValue()) {
              memberToGCVersion.put(entry.getKey(), entry.getValue());
            }
          }
        }
      }
    }
  }

  public void initializeVersionHolder(T mbr, RegionVersionHolder<T> otherHolder) {
    RegionVersionHolder<T> h = memberToVersion.get(mbr);
    if (h == null) {
      if (!mbr.equals(myId)) {
        h = otherHolder.clone();
        h.makeReadyForRecording();
        memberToVersion.put(mbr, h);
      } else {
        RegionVersionHolder<T> vh = otherHolder;
        long version = vh.version;
        updateLocalVersion(version);
        localExceptions.initializeFrom(vh);
      }
    } else {
      // holders must be modified under synchronization
      h.initializeFrom(otherHolder);
    }
  }

  void updateLocalVersion(long newVersion) {
    boolean needToTrySetAgain;
    do {
      needToTrySetAgain = false;
      long currentVersion = localVersion.get();
      if (currentVersion < newVersion) {
        needToTrySetAgain = !compareAndSetVersion(currentVersion, newVersion);
      }
    } while (needToTrySetAgain);
  }

  boolean compareAndSetVersion(long currentVersion, long newVersion) {
    return localVersion.compareAndSet(currentVersion, newVersion);
  }

  /**
   * Records a received region-version. These are transmitted in VersionTags in messages between
   * peers and from servers to clients.
   *
   * @param tag the version information
   */
  public void recordVersion(T mbr, VersionTag<T> tag) {
    tag.setRecorded();
    assert tag.isRecorded();
    T member = tag.getMemberID();
    if (member == null) {
      member = mbr;
    }

    if (myId.equals(member)) {
      // We can be asked to record a version for the local member if a persistent
      // member is restarted and an event is replayed after the persistent member
      // recovers. So we can only assert that the local member has already seen
      // the replayed event.
      synchronized (localExceptions) {
        if (localVersion.get() < tag.getRegionVersion() && region != null
            && region.isInitialized() && region.getDataPolicy().withPersistence()) {
          Assert.fail(
              "recordVersion invoked for a local version tag that is higher than our local version. rvv="
                  + this + ", tag=" + tag + " " + region.getName());
        }
      }
    }

    recordVersion(member, tag.getRegionVersion());
  }

  /**
   * Records a received region-version. These are transmitted in VersionTags in messages between
   * peers and from servers to clients. In general you should use recordVersion(mbr, versionTag) so
   * that the tag is marked as having been recorded. This will keep
   * DistributedCacheOperation.basicProcess() from trying to record it again.
   *
   * This method is also called for versions which have been recovered from disk.
   *
   * @param member the peer that performed the operation
   * @param version the version of the peers region that reflects the operation
   */
  public void recordVersion(T member, long version) {
    T mbr = member;

    if (recordingDisabled || clientVector) {
      return;
    }

    RegionVersionHolder<T> holder;

    if (mbr.equals(myId)) {
      // If we are recording a version for the local member,
      // use the local exception list.
      holder = localExceptions;

      synchronized (holder) {
        // Advance the version held in the local
        // exception list to match the atomic long
        // we using for the local version.
        holder.version = localVersion.get();
      }
      updateLocalVersion(version);
    } else {
      // Find the version holder object
      holder = memberToVersion.get(mbr);
      if (holder == null) {
        synchronized (memberToVersion) {
          // Look for the holder under lock
          holder = memberToVersion.get(mbr);
          if (holder == null) {
            mbr = getCanonicalId(mbr);
            holder = new RegionVersionHolder<>(mbr);
            memberToVersion.put(holder.id, holder);
          }
        }
      }
    }

    // Update the version holder
    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "Recording rv{} for {}", version, mbr);
    }
    holder.recordVersion(version);
  }



  /**
   * Records a version holder that we have recovered from disk. This version holder replaces the
   * current version holder if it dominates the version holder we already have. This method will
   * called once for each oplog we recover.
   *
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ML_SYNC_ON_FIELD_TO_GUARD_CHANGING_THAT_FIELD",
      justification = "sync on localExceptions guards concurrent modification but this is a replacement")
  public void initRecoveredVersion(T member, RegionVersionHolder<T> v, boolean latestOplog) {
    RegionVersionHolder<T> recovered = v.clone();

    if (member == null || member.equals(myId)) {
      // if this is the version for the local member, update our local info

      // update the local exceptions
      synchronized (localExceptions) {
        // Fix for 45622 - We only take the version holder from the latest
        // oplog. There may be more than one RVV in the latest oplog, in which
        // case we want to end up with the last RVV from the latest oplog
        if (latestOplog || localVersion.get() == 0) {
          localExceptions = recovered;
          if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
            logger.trace(LogMarker.RVV_VERBOSE, "initRecoveredVersion setting local version to {}",
                recovered.version);
          }
          localVersion.set(recovered.version);
        }
      }
    } else {
      // If this is not a local member, update the member to version map
      Long gcVersion = memberToGCVersion.get(member);

      synchronized (memberToVersion) {
        RegionVersionHolder<T> oldVersion = memberToVersion.get(member);

        // Fix for 45622 - We only take the version holder from the latest
        // oplog. There may be more than one RVV in the latest oplog, in which
        // case we want to end up with the last RVV from the latest oplog
        if (latestOplog || oldVersion == null || oldVersion.version == 0) {
          if (gcVersion != null) {
            recovered.removeExceptionsOlderThan(gcVersion);
          }
          memberToVersion.put(member, recovered);
        }
      }
    }
  }

  /**
   * get the last recorded region version number for the given member
   */
  public long getVersionForMember(T mbr) {
    RegionVersionHolder<T> holder = memberToVersion.get(mbr);
    if (holder == null) {
      if (mbr.equals(myId)) {
        return getCurrentVersion();
      } else {
        return 0;
      }
    } else {
      return holder.getVersion();
    }
  }

  /**
   * Returns a list of the members that have been marked as having left the system.
   */
  public Set<T> getDepartedMembersSet() {
    synchronized (memberToVersion) {
      Set<T> result = new HashSet<>();
      for (RegionVersionHolder<T> h : memberToVersion.values()) {
        if (h.isDepartedMember) {
          result.add(h.id);
        }
      }
      return result;
    }
  }


  /**
   * Test to see if this vector has seen the given version.
   *
   * @return true if this vector has seen the given version
   */
  public boolean contains(T id, long version) {
    RegionVersionHolder<T> holder = memberToVersion.get(id);
    // For region synchronization.
    if (isForSynchronization()) {
      if (holder == null) {
        // we only care about missing changes from a particular member, and this
        // vector is known to contain that member's version holder
        return true;
      }
      if (id.equals(myId)) {
        if (!myId.isDiskStoreId()) {
          // a sync vector only has one holder if not recovered from persistence,
          // no valid version for the vector's owner
          return true;
        }
      }
      return holder.contains(version);
    }

    // Regular GII
    if (id.equals(myId)) {
      if (getCurrentVersion() < version) {
        return false;
      } else {
        return !localExceptions.hasExceptionFor(version);
      }
    }
    if (holder == null) {
      return false;
    } else {
      return holder.contains(version);
    }
  }

  /**
   * Removes departed members not in the given collection of IDs from the version vector
   *
   * @param idsToKeep collection of the kind of IDs appropriate for this vector
   */
  public void removeOldMembers(Set<VersionSource<T>> idsToKeep) {
    synchronized (memberToVersion) {
      for (Iterator<Map.Entry<T, RegionVersionHolder<T>>> it =
          memberToVersion.entrySet().iterator(); it.hasNext();) {
        Map.Entry<T, RegionVersionHolder<T>> entry = it.next();
        if (entry.getValue().isDepartedMember) {
          if (!idsToKeep.contains(entry.getKey())) {
            it.remove();
            memberToGCVersion.remove(entry.getKey());
            synchronized (canonicalIdLock) {
              canonicalIds.remove(entry.getKey());
            }
          }
        }
      }
    }
  }



  /**
   * Test hook - does this vector hold an entry for the given ID?
   */
  public boolean containsMember(T id) {
    if (memberToVersion.containsKey(id)) {
      return true;
    }
    if (memberToGCVersion.containsKey(id)) {
      return true;
    }
    return canonicalIds.containsKey(id);
  }

  /**
   * This marks the given entry as departed, making it eligible to be removed during an operation
   * like DistributedRegion.synchronizeWith()
   *
   */
  protected void markDepartedMember(T id) {
    synchronized (memberToVersion) {
      RegionVersionHolder<T> holder = memberToVersion.get(id);
      if (holder != null) {
        holder.isDepartedMember = true;
      }
    }
  }

  /**
   * check to see if tombstone removal in this RVV indicates that tombstones have been removed from
   * its Region that have not been removed from the argument's Region. If this is the case, then a
   * delta GII may leave entries in the other RVV's Region that should be deleted.
   *
   * @return true if there have been tombstone removals in this vector's Region that were not done
   *         in the argument's region
   */
  public boolean hasHigherTombstoneGCVersions(RegionVersionVector<T> other) {
    if (localGCVersion.get() > 0) {
      Long version = other.memberToGCVersion.get(myId);
      if (version == null) {
        return true; // this vector has removed locally created tombstones that the other hasn't
                     // reaped
      } else if (localGCVersion.get() > version) {
        return true;
      }
    }
    // see if I have members with GC versions that the other vector doesn't have
    for (T mbr : memberToGCVersion.keySet()) {
      if (!other.memberToGCVersion.containsKey(mbr)) {
        if (!mbr.equals(other.getOwnerId())) {
          return true;
        }
      }
    }
    // see if the other vector has members that have been removed from this
    // vector. If this happens we don't know if tombstones were removed
    for (T id : other.memberToGCVersion.keySet()) {
      if (!id.equals(myId) && !memberToGCVersion.containsKey(id)) {
        return true;
      }
    }
    // now see if I have anything newer for things we have in common
    for (Map.Entry<T, Long> entry : other.memberToGCVersion.entrySet()) {
      Long version = memberToGCVersion.get(entry.getKey());
      if (version != null) {
        Long otherVersion = entry.getValue();
        if (version > otherVersion) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Test to see if this vector's region may be able to provide updates that the given vector has
   * not seen. This method assumes that the argument is not a live vector and requires no
   * synchronization.
   */
  public boolean isNewerThanOrCanFillExceptionsFor(RegionVersionVector<T> other) {
    if (other.singleMember) {
      // do the diff for only a single member. This is typically a member that
      // recently crashed.
      Map.Entry<T, RegionVersionHolder<T>> entry =
          other.memberToVersion.entrySet().iterator().next();
      RegionVersionHolder<T> holder = memberToVersion.get(entry.getKey());
      if (holder == null) {
        return false;
      } else {
        RegionVersionHolder<T> otherHolder = entry.getValue();
        return holder.isNewerThanOrCanFillExceptionsFor(otherHolder);
      }
    }
    // check my own updates
    if (getCurrentVersion() > 0) {
      RegionVersionHolder<T> otherHolder = other.memberToVersion.get(myId);
      if (otherHolder == null) {
        return true;
      }
      if (localExceptions.isNewerThanOrCanFillExceptionsFor(otherHolder)) {
        return true;
      }
    }
    // now see if I have anything newer for things we have in common
    for (Map.Entry<T, RegionVersionHolder<T>> entry : memberToVersion.entrySet()) {
      T mbr = entry.getKey();
      RegionVersionHolder<T> holder = entry.getValue();
      RegionVersionHolder<T> otherHolder = other.memberToVersion.get(mbr);
      if (otherHolder != null) {
        if (holder.isNewerThanOrCanFillExceptionsFor(otherHolder)) {
          return true;
        }
      } else if (mbr.equals(other.getOwnerId())) {
        if (holder.isNewerThanOrCanFillExceptionsFor(other.localExceptions)) {
          return true;
        }
      } else {
        // mbr = entry.getKey() does not exist in other
        return true;
      }
    }
    return false;
  }

  private boolean isGCVersionDominatedByOtherHolder(Long gcVersion,
      RegionVersionHolder<T> otherHolder) {
    if (gcVersion == null || gcVersion == 0) {
      return true;
    } else {
      RegionVersionHolder<T> holder = new RegionVersionHolder<>(gcVersion.longValue());
      return !holder.isNewerThanOrCanFillExceptionsFor(otherHolder);
    }
  }

  /**
   * See if this vector's rvvgc has updates that has not seen.
   */
  public synchronized boolean isRVVGCDominatedBy(RegionVersionVector<T> requesterRVV) {
    if (requesterRVV.singleMember) {
      // do the diff for only a single member. This is typically a member that
      // recently crashed.
      Map.Entry<T, RegionVersionHolder<T>> entry =
          requesterRVV.memberToVersion.entrySet().iterator().next();

      Long gcVersion = memberToGCVersion.get(entry.getKey());
      return isGCVersionDominatedByOtherHolder(gcVersion, entry.getValue());
    }

    boolean isDominatedByRemote = true;
    long localgcversion = localGCVersion.get();
    if (localgcversion > 0) {
      RegionVersionHolder<T> otherHolder = requesterRVV.memberToVersion.get(myId);
      isDominatedByRemote = isGCVersionDominatedByOtherHolder(localgcversion, otherHolder);
      if (isDominatedByRemote == false) {
        return false;
      }
    }

    for (Map.Entry<T, Long> entry : memberToGCVersion.entrySet()) {
      T mbr = entry.getKey();
      Long gcVersion = entry.getValue();
      RegionVersionHolder<T> otherHolder = null;
      if (mbr.equals(requesterRVV.getOwnerId())) {
        otherHolder = requesterRVV.localExceptions;
      } else {
        otherHolder = requesterRVV.memberToVersion.get(mbr);
      }
      isDominatedByRemote = isGCVersionDominatedByOtherHolder(gcVersion, otherHolder);
      if (isDominatedByRemote == false) {
        return false;
      }
    }
    return isDominatedByRemote;
  }

  /**
   * wait for this vector to dominate the given vector. This means that the receiver has seen all
   * version changes that the given vector has seen.
   *
   * @param otherVector the vector, usually from another member, that we want to dominate
   * @param region the region owning this vector
   * @return true if dominance was achieved
   */
  public boolean waitToDominate(RegionVersionVector<T> otherVector, LocalRegion region) {
    if (otherVector == this) {
      return true;
    }
    boolean result = false;
    long waitTimeRemaining = 0;
    long startTime = System.currentTimeMillis();
    boolean interrupted = false;
    CancelCriterion stopper = region.getCancelCriterion();
    try {
      do {
        stopper.checkCancelInProgress(null);
        result = dominates(otherVector);
        if (!result) {
          long now = System.currentTimeMillis();
          waitTimeRemaining = MAX_DOMINANCE_WAIT_TIME - (now - startTime);
          if (logger.isTraceEnabled()) {
            logger.trace("Waiting up to {} ms to achieve dominance", waitTimeRemaining);
          }
          if (waitTimeRemaining > 0) {
            long waitTime = Math.min(DOMINANCE_PAUSE_TIME, waitTimeRemaining);
            try {
              Thread.sleep(waitTime);
            } catch (InterruptedException e) {
              stopper.checkCancelInProgress(e);
              interrupted = true;
              break;
            }
          }
        }
      } while (waitTimeRemaining > 0 && !result);
    } finally {
      if (interrupted) {
        if (logger.isTraceEnabled()) {
          logger.trace("waitForDominance has been interrupted");
        }
        Thread.currentThread().interrupt();
      }
    }
    return result;
  }


  /** return true if this vector has seen all version changes that the other vector has seen */
  public boolean dominates(RegionVersionVector<T> other) {
    return !other.isNewerThanOrCanFillExceptionsFor(this);
  }

  /**
   * Remove any exceptions for the given member that are older than the given version. This is used
   * after a synchronization operation to get rid of unneeded history.
   *
   */
  public void removeExceptionsFor(DistributedMember mbr, long version) {
    RegionVersionHolder<T> holder = memberToVersion.get(mbr);
    if (holder != null) {
      synchronized (holder) {
        holder.removeExceptionsOlderThan(version);
      }
    }
  }

  /**
   * This is used by clear() while version generation is locked to remove old exceptions and update
   * the GC vector to be the same as the current version vector
   */
  public void removeOldVersions() {
    synchronized (memberToVersion) {
      long currentVersion = getCurrentVersion();
      for (Map.Entry<T, RegionVersionHolder<T>> entry : memberToVersion.entrySet()) {
        RegionVersionHolder<T> holder = entry.getValue();
        T id = entry.getKey();
        holder.removeExceptionsOlderThan(holder.version);
        memberToGCVersion.put(id, holder.version);
      }
      localGCVersion.set(getCurrentVersion());
      if (localExceptions != null) {
        synchronized (localExceptions) {
          localExceptions.removeExceptionsOlderThan(currentVersion);
        }
      }
    }
  }

  /**
   * Test hook
   */
  public int getExceptionCount(T mbr) {
    if (mbr.equals(myId)) {
      return localExceptions.getExceptionCount();
    }
    RegionVersionHolder<T> h = memberToVersion.get(mbr);
    if (h == null) {
      throw new IllegalStateException("there should be a holder for " + mbr);
    }
    return h.getExceptionCount();
  }

  /**
   * after deserializing a version tag or RVV the IDs in it should be replaced with references to
   * IDs returned by this method. This vastly reduces the memory footprint of tags/stamps/rvvs
   *
   * @return the canonical reference
   */
  public T getCanonicalId(T id) {
    if (id == null) {
      return null;
    } else if (id.equals(myId)) {
      return myId;
    } else {
      T can = id;
      T cId = canonicalIds.get(can);
      if (cId != null) {
        return cId;
      }
      if (!id.isDiskStoreId()) {
        InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
        if (system != null) {
          can = (T) system.getDistributionManager().getCanonicalId((InternalDistributedMember) id);
        }
      }
      synchronized (canonicalIdLock) {
        HashMap<T, T> tmp = new HashMap<>(canonicalIds);
        tmp.put(can, can);
        canonicalIds = tmp;
      }
      return can;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.serialization.DataSerializableFixedID#toData(java.io.DataOutput)
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    if (isLiveVector) {
      throw new IllegalStateException("serialization of this object is not allowed");
    }
    writeMember(myId, out);
    int flags = 0;
    if (singleMember) {
      flags |= 0x01;
    }
    out.writeInt(flags);
    out.writeLong(localVersion.get());
    out.writeLong(localGCVersion.get());
    out.writeInt(memberToVersion.size());
    for (Map.Entry<T, RegionVersionHolder<T>> entry : memberToVersion.entrySet()) {
      writeMember(entry.getKey(), out);
      InternalDataSerializer.invokeToData(entry.getValue(), out);
    }
    out.writeInt(memberToGCVersion.size());
    for (Map.Entry<T, Long> entry : memberToGCVersion.entrySet()) {
      writeMember(entry.getKey(), out);
      out.writeLong(entry.getValue());
    }
    InternalDataSerializer.invokeToData(localExceptions, out);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.serialization.DataSerializableFixedID#fromData(java.io.DataInput)
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    myId = readMember(in);
    int flags = in.readInt();
    singleMember = ((flags & 0x01) == 0x01);
    localVersion.set(in.readLong());
    localGCVersion.set(in.readLong());
    int numHolders = in.readInt();
    for (int i = 0; i < numHolders; i++) {
      T key = readMember(in);
      RegionVersionHolder<T> holder = new RegionVersionHolder<>(in);
      holder.id = key;
      memberToVersion.put(key, holder);
    }
    int numGCVersions = in.readInt();
    for (int i = 0; i < numGCVersions; i++) {
      T key = readMember(in);
      RegionVersionHolder<T> holder = memberToVersion.get(key);
      if (holder != null) {
        key = holder.id;
      } // else it could go in canonicalIds, but that's not used in copies of RVVs
      long value = in.readLong();
      memberToGCVersion.put(key, value);
    }
    localExceptions = new RegionVersionHolder<>(in);
  }

  protected abstract void writeMember(T member, DataOutput out) throws IOException;

  protected abstract T readMember(DataInput in) throws IOException, ClassNotFoundException;

  /**
   * When a tombstone is removed from the entry map this method must be called to record the max
   * region-version of any tombstone reaped. Any older versions are then immediately eligible for
   * reaping.
   *
   */
  public void recordGCVersion(T mbr, long regionVersion) {
    if (mbr == null) {
      mbr = myId;
    }
    // record the GC version to make sure we know we have seen this version
    // during recovery, this will prevent us from recording exceptions
    // for entries less than the GC version.
    recordVersion(mbr, regionVersion);
    if (mbr == null || mbr.equals(myId)) {
      boolean succeeded;
      do {
        long v = localGCVersion.get();
        if (v > regionVersion) {
          break;
        }
        succeeded = localGCVersion.compareAndSet(v, regionVersion);
      } while (!succeeded);
    } else {
      synchronized (memberToGCVersion) {
        Long holder = memberToGCVersion.get(mbr);
        if (holder != null) {
          memberToGCVersion.put(mbr, Math.max(regionVersion, holder));
        } else {
          memberToGCVersion.put(mbr, regionVersion);
        }
      }
    }
  }

  /**
   * record all of the GC versions in the given vector
   *
   */
  public void recordGCVersions(RegionVersionVector<T> other) {
    assert other.memberToGCVersion != null : "incoming gc version set is null";
    recordGCVersion(other.myId, other.localGCVersion.get());
    for (Map.Entry<T, Long> entry : other.memberToGCVersion.entrySet()) {
      recordGCVersion(entry.getKey(), entry.getValue());
    }
  }

  /**
   * returns true if tombstones newer than the given version have already been reaped. This means
   * that a clear or GC has been received that should have wiped out the operation this version
   * stamp represents, but this operation had not yet been received
   *
   * @return true if the given version should be rejected
   */
  public boolean isTombstoneTooOld(T mbr, long gcVersion) {
    Long newestReapedVersion;
    if (mbr == null || mbr.equals(myId)) {
      newestReapedVersion = localGCVersion.get();
    } else {
      newestReapedVersion = memberToGCVersion.get(mbr);
    }
    if (newestReapedVersion != null) {
      return (newestReapedVersion >= gcVersion);
    }
    return false;
  }

  /**
   * returns the highest region-version of any tombstone owned by the given member that was reaped
   * in this vector's region
   */
  public long getGCVersion(T mbr) {
    if (mbr == null || mbr.equals(myId)) {
      return localGCVersion.get();
    } else {
      synchronized (memberToGCVersion) {
        Long holder = memberToGCVersion.get(mbr);
        if (holder != null) {
          return holder;
        }
        return -1;
      }
    }
  }

  /**
   * Get a map of the member to the version and exception list for that member, including the local
   * member.
   */
  public Map<T, RegionVersionHolder<T>> getMemberToVersion() {
    RegionVersionHolder<T> myExceptions;
    myExceptions = localExceptions.clone();

    HashMap<T, RegionVersionHolder<T>> results =
        new HashMap<>(memberToVersion);

    results.put(getOwnerId(), myExceptions);
    return results;
  }

  /**
   * Get a map of member to the GC version of that member, including the local member.
   */
  public synchronized Map<T, Long> getMemberToGCVersion() {
    HashMap<T, Long> results = new HashMap<>(memberToGCVersion);
    if (localGCVersion.get() > 0) {
      results.put(getOwnerId(), localGCVersion.get());
    }
    return results;
  }

  /**
   * Remove an exceptions that are older than the current GC version for each member in the RVV.
   */
  public void pruneOldExceptions() {
    Set<T> members;
    members = new HashSet<>(memberToGCVersion.keySet());

    for (T member : members) {
      Long gcVersion = memberToGCVersion.get(member);

      RegionVersionHolder<T> holder;
      holder = memberToVersion.get(member);

      if (holder != null && gcVersion != null) {
        synchronized (holder) {
          holder.removeExceptionsOlderThan(gcVersion);
        }
      }
    }

    localExceptions.removeExceptionsOlderThan(localGCVersion.get());
  }

  @Override
  public String toString() {
    if (isLiveVector) {
      return "RegionVersionVector{rv" + localVersion + " gc" + localGCVersion + "}@"
          + System.identityHashCode(this);
    } else {
      return fullToString();
    }
  }

  /** this toString method is not thread-safe */
  public String fullToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RegionVersionVector[").append(myId).append("={rv")
        .append(localExceptions.version).append(" gc" + localGCVersion)
        .append(" localVersion=" + localVersion);
    try {
      sb.append(" local exceptions=" + localExceptions.exceptionsToString());
    } catch (ConcurrentModificationException c) {
      sb.append(" (unable to access local exceptions)");
    }
    sb.append("} others=");
    String mbrVersions = "";
    try {
      mbrVersions = memberToVersion.toString();
    } catch (ConcurrentModificationException e) {
      mbrVersions = "(unable to access)";
    }
    sb.append(mbrVersions);
    if (memberToGCVersion != null) {
      try {
        mbrVersions = memberToGCVersion.toString();
      } catch (ConcurrentModificationException e) {
        mbrVersions = "(unable to access)";
      }
      sb.append(", gc=").append(mbrVersions);
    }
    sb.append("]");
    return sb.toString();
  }


  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {}

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  /*
   * (non-Javadoc) this ensures that the version generation lock is released
   *
   * @see org.apache.geode.distributed.internal.MembershipListener#memberDeparted(org.apache.geode.
   * distributed.internal.membership.InternalDistributedMember, boolean)
   */
  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, boolean crashed) {
    // since unlockForClear uses synchronization we need to try to execute it in another
    // thread so that membership events aren't blocked
    if (distributionManager != null) {
      distributionManager.getExecutors().getWaitingThreadPool().execute(() -> unlockForClear(id));
    } else {
      unlockForClear(id);
    }
  }

  public static RegionVersionVector<?> create(boolean persistent, DataInput in)
      throws IOException, ClassNotFoundException {
    RegionVersionVector<?> rvv;
    if (persistent) {
      rvv = new DiskRegionVersionVector();
    } else {
      rvv = new VMRegionVersionVector();
    }
    InternalDataSerializer.invokeFromData(rvv, in);
    return rvv;
  }

  public static RegionVersionVector<?> create(VersionSource<?> versionMember, LocalRegion owner) {
    if (versionMember.isDiskStoreId()) {
      return new DiskRegionVersionVector((DiskStoreID) versionMember, owner);
    } else {
      return new VMRegionVersionVector((InternalDistributedMember) versionMember, owner);
    }
  }

  /**
   * For test purposes, see if two RVVs have seen the same events and GC version vectors
   *
   * @return true if the RVVs are the same.
   */
  public boolean sameAs(RegionVersionVector<T> other) {
    // Compare the version version vectors
    Map<T, RegionVersionHolder<T>> myMemberToVersion = getMemberToVersion();
    Map<T, RegionVersionHolder<T>> otherMemberToVersion = other.getMemberToVersion();

    if (!myMemberToVersion.keySet().equals(otherMemberToVersion.keySet())) {
      return false;
    }
    for (T key : myMemberToVersion.keySet()) {
      if (!myMemberToVersion.get(key).sameAs(otherMemberToVersion.get(key))) {
        return false;
      }
    }

    Map<T, Long> myGCVersion = getMemberToGCVersion();
    Map<T, Long> otherGCVersion = other.getMemberToGCVersion();

    return myGCVersion.equals(otherGCVersion);
  }

  /**
   * Note: test only. If put in production will cause ConcurrentModificationException see if two
   * RVVs have seen the same events and GC version vectors This will treat member with null version
   * the same as member with version=0
   *
   * @return true if the RVVs are the same logically.
   */
  public boolean logicallySameAs(RegionVersionVector<T> other) {
    return (dominates(other) && other.dominates(this));
  }


  /**
   * @return true if this vector represents the entry for a single member to be used in
   *         synchronizing caches for that member in the case of a crash. Otherwise return false.
   */
  public boolean isForSynchronization() {
    return singleMember;
  }

  /**
   * Test hook - see if a member is marked as "departed"
   */
  public boolean isDepartedMember(VersionSource<T> mbr) {
    RegionVersionHolder<T> h = memberToVersion.get(mbr);
    return (h != null) && h.isDepartedMember;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  // /**
  // * This class will wrap DM member IDs to provide integers that can be stored
  // * on disk and be timed out in the vector.
  // *
  // *
  // */
  // static class RVVMember implements Comparable {
  // private static AtomicLong NextId = new AtomicLong();
  // T memberId;
  // long timeAdded;
  // long internalId;
  //
  // RVVMember(T m, long timeAdded, long internalId) {
  // this.memberId = m;
  // this.timeAdded = timeAdded;
  // this.internalId = internalId;
  // if (NextId.get() < internalId) {
  // NextId.set(internalId);
  // }
  // }
  //
  // RVVMember(T m) {
  // this.memberId = m;
  // this.timeAdded = System.currentTimeMillis();
  // this.internalId = NextId.incrementAndGet();
  // }
  //
  // public int compareTo(Object o) {
  // if (o instanceof T) {
  // return -((T)o).compareTo(this.memberId);
  // } else {
  // return this.memberId.compareTo(((RVVMember)o).memberId);
  // }
  // }
  //
  // @Override
  // public int hashCode() {
  // return this.memberId.hashCode();
  // }
  //
  // @Override
  // public boolean equals(Object o) {
  // if (o instanceof T) {
  // return ((T)o).equals(this.memberId);
  // } else {
  // return this.memberId.equals(((RVVMember)o).memberId);
  // }
  // }
  //
  // @Override
  // public String toString() {
  // return "vID(#"+this.internalId+"; time="+this.timeAdded+"; id="+this.memberId+")";
  // }
  // }



}
