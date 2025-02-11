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
package org.apache.geode.internal.cache.entries;

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ABSTRACT_REGION_ENTRY_FILL_IN_VALUE;
import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.IndexMaintenanceException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.util.GatewayConflictHelper;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.ImageState;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TimestampedEntryEventImpl;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.ValueComparisonHelper;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.ReferenceCountHelper;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.Versionable;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.ConvertableToBytes;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Abstract implementation class of RegionEntry interface. This is the topmost implementation class
 * so common behavior lives here.
 *
 * @since GemFire 3.5.1
 */
public abstract class AbstractRegionEntry implements HashRegionEntry<Object, Object> {
  private static final Logger logger = LogService.getLogger();

  /**
   * Whether to disable last access time update when a put occurs. The default is false (enable last
   * access time update on put). To disable it, set the 'gemfire.disableAccessTimeUpdateOnPut'
   * system property.
   */
  protected static final boolean DISABLE_ACCESS_TIME_UPDATE_ON_PUT =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disableAccessTimeUpdateOnPut");

  /*
   * Flags for a Region Entry. These flags are stored in the msb of the long used to also store the
   * lastModificationTime.
   */
  private static final long VALUE_RESULT_OF_SEARCH = 0x01L << 56;

  private static final long UPDATE_IN_PROGRESS = 0x02L << 56;

  private static final long LISTENER_INVOCATION_IN_PROGRESS = 0x08L << 56;

  /** used for LRUEntry instances. */
  protected static final long RECENTLY_USED = 0x10L << 56;

  /** used for LRUEntry instances. */
  protected static final long EVICTED = 0x20L << 56;

  /**
   * Set if the entry is being used by a transactions. Some features (eviction and expiration) will
   * not modify an entry when a tx is using it to prevent the tx to fail do to conflict.
   */
  private static final long IN_USE_BY_TX = 0x40L << 56;

  protected AbstractRegionEntry(RegionEntryContext context,
      @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE) Object value) {

    setValue(context, prepareValueForCache(context, value, false), false);

    // setLastModified(System.currentTimeMillis()); this must be set later so we can use ==0
    // to know this is a new entry in checkForConflicts
  }

  @Override
  @SuppressWarnings("IMSE_DONT_CATCH_IMSE")
  public boolean dispatchListenerEvents(final EntryEventImpl event) throws InterruptedException {
    final InternalRegion rgn = event.getRegion();

    if (event.callbacksInvoked()) {
      return true;
    }

    // don't wait for certain events to reach the head of the queue before
    // dispatching listeners. However, we must not notify the gateways for
    // remote-origin ops out of order. Otherwise the other systems will have
    // inconsistent content.

    event.setCallbacksInvokedByCurrentThread();

    if (logger.isDebugEnabled() && !rgn.isInternalRegion()) {
      logger.debug("{} dispatching event {}", this, event);
    }
    // All the following code that sets "thr" is to workaround
    // spurious IllegalMonitorStateExceptions caused by JVM bugs.
    try {
      // call invokeCallbacks while synced on RegionEntry
      event.invokeCallbacks(rgn, event.inhibitCacheListenerNotification(), false);
      return true;

    } finally {
      if (isRemoved() && !isTombstone() && !event.isEvicted()) {
        // Phase 2 of region entry removal is done here. The first phase is done
        // by the RegionMap. It is unclear why this code is needed. ARM destroy
        // does this also and we are now doing it as phase3 of the ARM destroy.
        removePhase2();
        ((DiskRecoveryStore) rgn).getRegionMap().removeEntry(event.getKey(), this, true, event,
            rgn);
      }
    }
  }

  @Override
  public long getLastAccessed() throws InternalStatisticsDisabledException {
    throw new InternalStatisticsDisabledException();
  }

  @Override
  public long getHitCount() throws InternalStatisticsDisabledException {
    throw new InternalStatisticsDisabledException();
  }

  @Override
  public long getMissCount() throws InternalStatisticsDisabledException {
    throw new InternalStatisticsDisabledException();
  }

  /**
   * This sets the lastModified time for the entry. In subclasses with statistics it will also set
   * the lastAccessed time unless the system property gemfire.disableAccessTimeUpdateOnPut is set to
   * true.
   *
   * @param lastModified the time of last modification of the entry
   */
  public void setLastModified(long lastModified) {
    setLastModifiedAndAccessedTimes(lastModified, lastModified);
  }

  /**
   * This sets the lastModified and lastAccessed time for the entry. Subclasses that do not keep
   * track of lastAccessed time will ignore the second parameter.
   *
   * @param lastModified the time of last modification of the entry
   * @param lastAccessed the time the entry was last accessed
   */
  protected void setLastModifiedAndAccessedTimes(long lastModified, long lastAccessed) {
    _setLastModified(lastModified);
  }

  @Override
  public void txDidDestroy(long currentTime) {
    setLastModifiedAndAccessedTimes(currentTime, currentTime);
  }

  @Override
  public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
    setLastModifiedAndAccessedTimes(lastModifiedTime, lastAccessedTime);
  }

  @Override
  public void setRecentlyUsed(RegionEntryContext context) {
    // do nothing by default; only needed for LRU
  }

  @Override
  public void updateStatsForGet(boolean hit, long time) {
    // nothing needed
  }

  @Override
  public void resetCounts() throws InternalStatisticsDisabledException {
    throw new InternalStatisticsDisabledException();
  }

  void _removePhase1() {
    _setValue(Token.REMOVED_PHASE1);
  }

  @Override
  public void removePhase1(InternalRegion region, boolean clear) throws RegionClearedException {
    _removePhase1();
  }

  @Override
  public void removePhase2() {
    _setValue(Token.REMOVED_PHASE2);
  }

  @Override
  public void makeTombstone(InternalRegion region, VersionTag version)
      throws RegionClearedException {
    assert region.getVersionVector() != null;
    assert version != null;

    boolean wasTombstone = isTombstone();
    setRecentlyUsed(region);
    boolean newEntry = getValueAsToken() == Token.REMOVED_PHASE1;
    basicMakeTombstone(region);
    region.scheduleTombstone(this, version, wasTombstone);
    if (newEntry) {
      // bug #46631 - entry count is decremented by scheduleTombstone but this is a new entry
      region.getCachePerfStats().incEntryCount(1);
    }
  }

  private void basicMakeTombstone(InternalRegion region) throws RegionClearedException {
    boolean setValueCompleted = false;
    try {
      setValue(region, Token.TOMBSTONE);
      setValueCompleted = true;
    } finally {
      if (!setValueCompleted && isTombstone()) {
        removePhase2();
      }
    }
  }

  @Override
  public void setValueWithTombstoneCheck(@Unretained Object v, EntryEvent e)
      throws RegionClearedException {
    if (v == Token.TOMBSTONE) {
      makeTombstone((InternalRegion) e.getRegion(), ((InternalCacheEvent) e).getVersionTag());
    } else {
      setValue((RegionEntryContext) e.getRegion(), v, (EntryEventImpl) e);
    }
  }

  /**
   * Return true if the object is removed.
   *
   * TODO this method does NOT return true if the object is Token.DESTROYED. dispatchListenerEvents
   * relies on that fact to avoid removing destroyed tokens from the map. We should refactor so that
   * this method calls Token.isRemoved, and places that don't want a destroyed Token can explicitly
   * check for a DESTROY token.
   */
  @Override
  public boolean isRemoved() {
    Token o = getValueAsToken();
    return o == Token.REMOVED_PHASE1 || o == Token.REMOVED_PHASE2 || o == Token.TOMBSTONE;
  }

  @Override
  public boolean isDestroyedOrRemoved() {
    return Token.isRemoved(getValueAsToken());
  }

  @Override
  public boolean isDestroyedOrRemovedButNotTombstone() {
    Token o = getValueAsToken();
    return o == Token.DESTROYED || o == Token.REMOVED_PHASE1 || o == Token.REMOVED_PHASE2;
  }

  @Override
  public boolean isTombstone() {
    return getValueAsToken() == Token.TOMBSTONE;
  }

  @Override
  public boolean isRemovedPhase2() {
    return getValueAsToken() == Token.REMOVED_PHASE2;
  }

  @Override
  public boolean fillInValue(InternalRegion region,
      @Retained(ABSTRACT_REGION_ENTRY_FILL_IN_VALUE) Entry entry, ByteArrayDataInput in,
      DistributionManager mgr, final KnownVersion version) {

    // starting default value
    entry.setSerialized(false);

    @Retained(ABSTRACT_REGION_ENTRY_FILL_IN_VALUE)
    final Object v;
    if (isTombstone()) {
      v = Token.TOMBSTONE;
    } else {
      // OFFHEAP: need to incrc, copy bytes, decrc
      v = getValue(region);
      if (v == null) {
        return false;
      }
    }

    entry.setLastModified(getLastModified());
    if (v == Token.INVALID) {
      entry.setInvalid();
    } else if (v == Token.LOCAL_INVALID) {
      entry.setLocalInvalid();
    } else if (v == Token.TOMBSTONE) {
      entry.setTombstone();
    } else if (v instanceof CachedDeserializable) {
      // don't serialize here if it is not already serialized
      CachedDeserializable cd = (CachedDeserializable) v;
      if (!cd.isSerialized()) {
        entry.setValue(cd.getDeserializedForReading());
      } else {
        Object tmp = cd.getValue();
        if (tmp instanceof byte[]) {
          entry.setValue(tmp);
        } else {
          try {
            HeapDataOutputStream hdos = new HeapDataOutputStream(version);
            BlobHelper.serializeTo(tmp, hdos);
            hdos.trim();
            entry.setValue(hdos);
          } catch (IOException e) {
            throw new IllegalArgumentException(
                "An IOException was thrown while serializing.",
                e);
          }
        }
        entry.setSerialized(true);
      }
    } else if (v instanceof byte[]) {
      entry.setValue(v);
    } else {
      Object preparedValue = v;
      if (preparedValue != null) {
        preparedValue = prepareValueForGII(preparedValue);
        if (preparedValue == null) {
          return false;
        }
      }
      try {
        HeapDataOutputStream hdos = new HeapDataOutputStream(version);
        BlobHelper.serializeTo(preparedValue, hdos);
        hdos.trim();
        entry.setValue(hdos);
        entry.setSerialized(true);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "An IOException was thrown while serializing.",
            e);
      }
    }
    return true;
  }

  /**
   * To fix bug 49901 if v is a GatewaySenderEventImpl then make a heap copy of it if it is offheap.
   *
   * @return the value to provide to the gii request; null if no value should be provided.
   */
  static Object prepareValueForGII(Object v) {
    assert v != null;
    if (v instanceof GatewaySenderEventImpl) {
      return ((GatewaySenderEventImpl) v).makeHeapCopyIfOffHeap();
    } else {
      return v;
    }
  }

  @Override
  public boolean isOverflowedToDisk(InternalRegion region,
      DistributedRegion.DiskPosition diskPosition) {
    return false;
  }

  @Override
  public Object getValue(RegionEntryContext context) {
    ReferenceCountHelper.createReferenceCountOwner();
    @Retained
    Object result = getValueRetain(context, true);

    // If the thread is an Index Creation Thread & the value obtained is
    // Token.REMOVED , we can skip synchronization block. This is required to prevent
    // the dead lock caused if an Index Update Thread has gone into a wait holding the
    // lock of the Entry object. There should not be an issue if the Index creation thread
    // gets the temporary value of token.REMOVED as the correct value will get indexed
    // by the Index Update Thread , once the index creation thread has exited.
    // Part of Bugfix # 33336

    if (Token.isRemoved(result)) {
      ReferenceCountHelper.setReferenceCountOwner(null);
      return null;
    } else {
      result = OffHeapHelper.copyAndReleaseIfNeeded(result, context.getCache());
      ReferenceCountHelper.setReferenceCountOwner(null);
      setRecentlyUsed(context);
      return result;
    }
  }

  @Override
  @Retained
  public Object getValueRetain(RegionEntryContext context) {
    @Retained
    Object result = getValueRetain(context, true);
    if (Token.isRemoved(result)) {
      return null;
    } else {
      setRecentlyUsed(context);
      return result;
    }
  }

  @Override
  @Released
  public void setValue(RegionEntryContext context, @Unretained Object value)
      throws RegionClearedException {
    // TODO: This will mark new entries as being recently used
    // It might be better to only mark them when they are modified.
    // Or should we only mark them on reads?
    setValue(context, value, true);
  }

  @Override
  public void setValue(RegionEntryContext context, Object value, EntryEventImpl event)
      throws RegionClearedException {
    setValue(context, value);
  }

  @Released
  protected void setValue(RegionEntryContext context, @Unretained Object value,
      boolean recentlyUsed) {
    _setValue(value);
    releaseOffHeapRefIfRegionBeingClosedOrDestroyed(context, value);
    if (recentlyUsed) {
      setRecentlyUsed(context);
    }
  }

  void releaseOffHeapRefIfRegionBeingClosedOrDestroyed(RegionEntryContext context, Object ref) {
    if (isOffHeapReference(ref) && isThisRegionBeingClosedOrDestroyed(context)) {
      ((Releasable) this).release();
    }
  }

  private boolean isThisRegionBeingClosedOrDestroyed(RegionEntryContext context) {
    return context instanceof InternalRegion
        && ((InternalRegion) context).isThisRegionBeingClosedOrDestroyed();
  }

  private boolean isOffHeapReference(Object ref) {
    return ref != Token.REMOVED_PHASE1 && this instanceof OffHeapRegionEntry
        && ref instanceof StoredObject && ((StoredObject) ref).hasRefCount();
  }

  /**
   * This method determines if the value is in a compressed representation and decompresses it if it
   * is.
   *
   * @param context the values context.
   * @param value a region entry value.
   *
   * @return the decompressed form of the value parameter.
   */
  static Object decompress(RegionEntryContext context, Object value) {
    if (isCompressible(context, value)) {
      long time = context.getCachePerfStats().startDecompression();
      value = EntryEventImpl.deserialize(context.getCompressor().decompress((byte[]) value));
      context.getCachePerfStats().endDecompression(time);
    }

    return value;
  }

  protected static Object compress(RegionEntryContext context, Object value) {
    return compress(context, value, null);
  }

  /**
   * This method determines if the value is compressible and compresses it if it is.
   *
   * @param context the values context.
   * @param value a region entry value.
   *
   * @return the compressed form of the value parameter.
   */
  protected static Object compress(RegionEntryContext context, Object value, EntryEventImpl event) {
    if (isCompressible(context, value)) {
      long time = context.getCachePerfStats().startCompression();
      byte[] serializedValue;
      if (event != null && event.getCachedSerializedNewValue() != null) {
        serializedValue = event.getCachedSerializedNewValue();
        if (value instanceof CachedDeserializable) {
          CachedDeserializable cd = (CachedDeserializable) value;
          if (!(cd.getValue() instanceof byte[])) {
            // The cd now has the object form so use the cached serialized form in a new cd.
            // This serialization is much cheaper than reserializing the object form.
            serializedValue = EntryEventImpl
                .serialize(CachedDeserializableFactory.create(serializedValue, context.getCache()));
          } else {
            serializedValue = EntryEventImpl.serialize(cd);
          }
        }
      } else {
        serializedValue = EntryEventImpl.serialize(value);
        if (event != null && !(value instanceof byte[])) {
          // See if we can cache the serialized new value in the event.
          // If value is a byte[] then we don't cache it since it is not serialized.
          if (value instanceof CachedDeserializable) {
            // For a CacheDeserializable we want to only cache the wrapped value;
            // not the serialized CacheDeserializable.
            CachedDeserializable cd = (CachedDeserializable) value;
            Object cdVal = cd.getValue();
            if (cdVal instanceof byte[]) {
              event.setCachedSerializedNewValue((byte[]) cdVal);
            }
          } else {
            event.setCachedSerializedNewValue(serializedValue);
          }
        }
      }
      value = context.getCompressor().compress(serializedValue);
      context.getCachePerfStats().endCompression(time, serializedValue.length,
          ((byte[]) value).length);
    }

    return value;
  }

  private static byte[] compressBytes(RegionEntryContext context, byte[] uncompressedBytes) {
    byte[] result = uncompressedBytes;
    if (isCompressible(context, uncompressedBytes)) {
      long time = context.getCachePerfStats().startCompression();
      result = context.getCompressor().compress(uncompressedBytes);
      context.getCachePerfStats().endCompression(time, uncompressedBytes.length, result.length);
    }
    return result;
  }


  @Override
  public Object getValueInVM(RegionEntryContext context) {
    ReferenceCountHelper.createReferenceCountOwner();
    @Released
    Object v = getValueRetain(context, true);

    if (v == null) {
      // should only be possible if disk entry
      v = Token.NOT_AVAILABLE;
    }
    Object result = OffHeapHelper.copyAndReleaseIfNeeded(v, context.getCache());
    ReferenceCountHelper.setReferenceCountOwner(null);
    return result;
  }

  @Override
  public Object getValueInVMOrDiskWithoutFaultIn(InternalRegion region) {
    return getValueInVM(region);
  }

  @Override
  @Retained
  public Object getValueOffHeapOrDiskWithoutFaultIn(InternalRegion region) {
    @Retained
    Object result = getValueRetain(region, true);
    return result;
  }

  @Override
  public Object getValueOnDisk(InternalRegion region) throws EntryNotFoundException {
    throw new IllegalStateException(
        "Cannot get value on disk for a region that does not access the disk.");
  }

  @Override
  public Object getSerializedValueOnDisk(final InternalRegion region)
      throws EntryNotFoundException {
    throw new IllegalStateException(
        "Cannot get value on disk for a region that does not access the disk.");
  }

  @Override
  public Object getValueOnDiskOrBuffer(InternalRegion region) throws EntryNotFoundException {
    throw new IllegalStateException(
        "Cannot get value on disk for a region that does not access the disk.");
    // TODO: if value is Token.REMOVED || Token.DESTROYED throw EntryNotFoundException
  }

  @Override
  public boolean initialImagePut(final InternalRegion region, final long lastModified,
      Object newValue, boolean wasRecovered, boolean acceptedVersionTag)
      throws RegionClearedException {
    // note that the caller has already write synced this RegionEntry
    return initialImageInit(region, lastModified, newValue, isTombstone(), wasRecovered,
        acceptedVersionTag);
  }

  @Override
  public boolean initialImageInit(final InternalRegion region, final long lastModified,
      final Object newValue, final boolean create, final boolean wasRecovered,
      final boolean acceptedVersionTag) throws RegionClearedException {

    // note that the caller has already write synced this RegionEntry
    boolean result = false;

    // if it has been destroyed then don't do anything
    Token vTok = getValueAsToken();
    if (acceptedVersionTag || create || (vTok != Token.DESTROYED || vTok != Token.TOMBSTONE)) {
      // OFFHEAP noop
      Object newValueToWrite = newValue;
      // OFFHEAP noop
      boolean putValue = acceptedVersionTag || create || (newValueToWrite != Token.LOCAL_INVALID
          && (wasRecovered || (vTok == Token.LOCAL_INVALID)));

      if (region.isUsedForPartitionedRegionAdmin()
          && newValueToWrite instanceof CachedDeserializable) {
        // Special case for partitioned region meta data
        // We do not need the RegionEntry on this case.
        // Because the pr meta data region will not have an LRU.
        newValueToWrite =
            ((CachedDeserializable) newValueToWrite).getDeserializedValue(region, null);
        if (!create && newValueToWrite instanceof Versionable) {
          // Heap value should always be deserialized at this point // OFFHEAP will not be
          // deserialized
          final Object oldValue = getValueInVM(region);
          // BUGFIX for 35029. If oldValue is null the newValue should be put.
          if (oldValue == null) {
            putValue = true;
          } else if (oldValue instanceof Versionable) {
            Versionable nv = (Versionable) newValueToWrite;
            Versionable ov = (Versionable) oldValue;
            putValue = nv.isNewerThan(ov);
          }
        }
      }

      if (putValue) {
        // change to INVALID if region itself has been invalidated,
        // and current value is recovered
        if (create || acceptedVersionTag) {
          // At this point, since we now always recover from disk first,
          // we only care about "isCreate" since "isRecovered" is impossible
          // if we had a regionInvalidate or regionClear
          ImageState imageState = region.getImageState();
          // this method is called during loadSnapshot as well as getInitialImage
          if (imageState.getRegionInvalidated()) {
            if (newValueToWrite != Token.TOMBSTONE) {
              newValueToWrite = Token.INVALID;
            }
          } else if (imageState.getClearRegionFlag()) {
            boolean entryOK = false;
            RegionVersionVector rvv = imageState.getClearRegionVersionVector();
            if (rvv != null) { // a filtered clear
              VersionSource id = getVersionStamp().getMemberID();
              if (id == null) {
                id = region.getVersionMember();
              }
              if (!rvv.contains(id, getVersionStamp().getRegionVersion())) {
                entryOK = true;
              }
            }
            if (!entryOK) {
              // If the region has been issued cleared during
              // the GII , then those entries loaded before this one would have
              // been cleared from the Map due to clear operation & for the
              // currententry whose key may have escaped the clearance , will be
              // cleansed by the destroy token.
              newValueToWrite = Token.DESTROYED; // TODO: never used
              imageState.addDestroyedEntry(getKey());
              throw new RegionClearedException(
                  "During the GII put of entry, the region got cleared so aborting the operation");
            }
          }
        }
        setValue(region, prepareValueForCache(region, newValueToWrite, false));
        result = true;

        if (newValueToWrite != Token.TOMBSTONE) {
          if (create) {
            region.getCachePerfStats().incCreates();
          }
          region.updateStatsForPut(this, lastModified, false);
        }

        if (logger.isTraceEnabled()) {
          if (newValueToWrite instanceof CachedDeserializable) {
            logger.trace("ProcessChunk: region={}; put a CachedDeserializable ({},{})",
                region.getFullPath(), getKey(),
                ((CachedDeserializable) newValueToWrite).getStringForm());
          } else {
            logger.trace("ProcessChunk: region={}; put({},{})", region.getFullPath(), getKey(),
                StringUtils.forceToString(newValueToWrite));
          }
        }
      }
    }
    return result;
  }

  /**
   * @throws EntryNotFoundException if expectedOldValue is not null and is not equal to current
   *         value
   */
  @Override
  @Released
  public boolean destroy(InternalRegion region, EntryEventImpl event, boolean inTokenMode,
      boolean cacheWrite, @Unretained Object expectedOldValue, boolean forceDestroy,
      boolean removeRecoveredEntry) throws CacheWriterException, EntryNotFoundException,
      TimeoutException, RegionClearedException {

    // A design decision was made to not retrieve the old value from the disk
    // if the entry has been evicted to only have the CacheListener afterDestroy
    // method ignore it. We don't want to pay the performance penalty. The
    // getValueInVM method does not retrieve the value from disk if it has been
    // evicted. Instead, it uses the NotAvailable token.
    //
    // If the region is a WAN queue region, the old value is actually used by the
    // afterDestroy callback on a secondary. It is not needed on a primary.
    // Since the destroy that sets WAN_QUEUE_TOKEN always originates on the primary
    // we only pay attention to WAN_QUEUE_TOKEN if the event is originRemote.
    //
    // We also read old value from disk or buffer
    // in the case where there is a non-null expectedOldValue
    // see PartitionedRegion#remove(Object key, Object value)
    ReferenceCountHelper.skipRefCountTracking();
    @Retained
    @Released
    Object curValue = getValueRetain(region, true);
    ReferenceCountHelper.unskipRefCountTracking();
    boolean proceed;
    try {
      if (curValue == null) {
        curValue = Token.NOT_AVAILABLE;
      }

      if (curValue == Token.NOT_AVAILABLE) {
        // In some cases we need to get the current value off of disk.

        // if the event is transmitted during GII and has an old value, it was
        // the state of the transmitting cache's entry & should be used here
        if (event.getCallbackArgument() != null
            && event.getCallbackArgument().equals(RegionQueue.WAN_QUEUE_TOKEN)
            && event.isOriginRemote()) { // check originRemote for bug 40508
          // curValue = getValue(region); can cause deadlock if GII is occurring
          curValue = getValueOnDiskOrBuffer(region);
        } else {
          FilterProfile fp = region.getFilterProfile();
          if (fp != null && (fp.getCqCount() > 0 || expectedOldValue != null)) {
            // curValue = getValue(region); can cause deadlock will fault in the value
            // and will confuse LRU.
            curValue = getValueOnDiskOrBuffer(region);
          }
        }
      }

      if (expectedOldValue != null) {
        if (!checkExpectedOldValue(expectedOldValue, curValue, region)) {
          throw new EntryNotFoundException(
              "The current value was not equal to expected value.");
        }
      }

      if (inTokenMode && event.hasOldValue()) {
        proceed = true;
      } else {
        event.setOldValue(curValue, curValue instanceof GatewaySenderEventImpl);
        proceed = region.getConcurrencyChecksEnabled() || removeRecoveredEntry || forceDestroy
            || destroyShouldProceedBasedOnCurrentValue(curValue)
            || (event.getOperation() == Operation.REMOVE && (curValue == null
                || curValue == Token.LOCAL_INVALID || curValue == Token.INVALID));
      }
    } finally {
      OffHeapHelper.releaseWithNoTracking(curValue);
    }

    if (proceed) {
      // Generate the version tag if needed. This method should only be
      // called if we are in fact going to destroy the entry, so it must be
      // after the entry not found exception above.
      if (!removeRecoveredEntry) {
        region.generateAndSetVersionTag(event, this);
      }
      if (cacheWrite) {
        region.cacheWriteBeforeDestroy(event, expectedOldValue);
        if (event.getRegion().getServerProxy() != null) {
          // server will return a version tag
          // update version information (may throw ConcurrentCacheModificationException)
          VersionStamp stamp = getVersionStamp();
          if (stamp != null) {
            stamp.processVersionTag(event);
          }
        }
      }
      region.recordEvent(event);
      // don't do index maintenance on a destroy if the value in the
      // RegionEntry (the old value) is invalid
      updateIndexOnDestroyOperation(region);

      boolean removeEntry = false;
      VersionTag v = event.getVersionTag();
      if (region.getConcurrencyChecksEnabled() && !removeRecoveredEntry
          && !event.isFromRILocalDestroy()) {
        // bug #46780, don't retain tombstones for entries destroyed for register-interest
        // Destroy will write a tombstone instead
        if (v == null || !v.hasValidVersion()) {
          // localDestroy and eviction and ops received with no version tag
          // should create a tombstone using the existing version stamp, as should
          // (bug #45245) responses from servers that do not have valid version information
          VersionStamp stamp = getVersionStamp();
          if (stamp != null) { // proxy has no stamps
            v = stamp.asVersionTag();
            event.setVersionTag(v);
          }
        }
        removeEntry = v == null || !v.hasValidVersion();
      } else {
        removeEntry = true;
      }

      if (removeEntry) {
        boolean isThisTombstone = isTombstone();
        if (inTokenMode && !event.getOperation().isEviction()) {
          setValue(region, Token.DESTROYED);
        } else {
          removePhase1(region, false);
        }
        if (isThisTombstone) {
          region.unscheduleTombstone(this);
        }
      } else {
        makeTombstone(region, v);
      }

      return true;
    } else {
      return false;
    }
  }

  protected void updateIndexOnDestroyOperation(InternalRegion region) {
    if (!isTombstone() && !region.isProxy() && !isInvalid()) {
      IndexManager indexManager = region.getIndexManager();
      if (indexManager != null) {
        try {
          if (isValueNull()) {
            @Released
            Object value = getValueOffHeapOrDiskWithoutFaultIn(region);
            try {
              Object preparedValue = prepareValueForCache(region, value, false);
              _setValue(preparedValue);
              releaseOffHeapRefIfRegionBeingClosedOrDestroyed(region, preparedValue);
            } finally {
              OffHeapHelper.release(value);
            }
          }
          indexManager.updateIndexes(this, IndexManager.REMOVE_ENTRY, IndexProtocol.OTHER_OP);
        } catch (QueryException e) {
          throw new IndexMaintenanceException(e);
        }
      }
    }
  }

  private static boolean destroyShouldProceedBasedOnCurrentValue(Object curValue) {
    if (curValue == null) {
      return false;
    }
    return !Token.isRemoved(curValue);
  }

  public static boolean checkExpectedOldValue(@Unretained Object expectedOldValue,
      @Unretained Object actualValue, InternalRegion region) {

    if (Token.isInvalid(expectedOldValue)) {
      return actualValue == null || Token.isInvalid(actualValue);
    } else {
      boolean isCompressedOffHeap =
          region.getAttributes().getOffHeap() && region.getAttributes().getCompressor() != null;
      return ValueComparisonHelper
          .checkEquals(expectedOldValue, actualValue, isCompressedOffHeap, region.getCache());
    }
  }

  // Do not add any instance fields to this class.
  // Instead add them to LeafRegionEntry.cpp

  public static class HashRegionEntryCreator
      implements CustomEntryConcurrentHashMap.HashEntryCreator<Object, Object> {

    @Override
    public HashEntry<Object, Object> newEntry(final Object key, final int hash,
        final HashEntry<Object, Object> next, final Object value) {
      final AbstractRegionEntry entry = (AbstractRegionEntry) value;
      // if hash is already set then assert that the two should be same
      final int entryHash = entry.getEntryHash();
      if (hash == 0 || entryHash != 0) {
        if (entryHash != hash) {
          Assert.fail("unexpected mismatch of hash, expected=" + hash + ", actual=" + entryHash
              + " for " + entry);
        }
      }
      entry.setEntryHash(hash);
      entry.setNextEntry(next);
      return entry;
    }

    @Override
    public int keyHashCode(final Object key, final boolean compareValues) {
      return CustomEntryConcurrentHashMap.keyHash(key, compareValues);
    }
  }

  @Override
  public abstract Object getKey();

  private static boolean okToStoreOffHeap(Object v, AbstractRegionEntry e) {
    if (v == null) {
      return false;
    }
    if (Token.isInvalidOrRemoved(v)) {
      return false;
    }
    if (v == Token.NOT_AVAILABLE) {
      return false;
    }
    if (v instanceof DiskEntry.RecoveredEntry) {
      return false; // The disk layer has special logic that ends up storing the nested value in the
    }
    // RecoveredEntry off heap
    return e instanceof OffHeapRegionEntry;
    // TODO should we check for deltas here or is that a user error?
  }

  /**
   * Default implementation. Override in subclasses with primitive keys to prevent creating an
   * Object form of the key for each equality check.
   */
  @Override
  public boolean isKeyEqual(Object k) {
    return k.equals(getKey());
  }

  private static final long LAST_MODIFIED_MASK = 0x00FFFFFFFFFFFFFFL;

  protected void _setLastModified(long lastModifiedTime) {
    if (lastModifiedTime < 0 || lastModifiedTime > LAST_MODIFIED_MASK) {
      throw new IllegalStateException("Expected lastModifiedTime " + lastModifiedTime
          + " to be >= 0 and <= " + LAST_MODIFIED_MASK);
    }
    long storedValue;
    long newValue;
    do {
      storedValue = getLastModifiedField();
      newValue = storedValue & ~LAST_MODIFIED_MASK;
      newValue |= lastModifiedTime;
    } while (!compareAndSetLastModifiedField(storedValue, newValue));
  }

  protected abstract long getLastModifiedField();

  protected abstract boolean compareAndSetLastModifiedField(long expectedValue, long newValue);

  @Override
  public long getLastModified() {
    return getLastModifiedField() & LAST_MODIFIED_MASK;
  }

  protected boolean areAnyBitsSet(long bitMask) {
    return (getLastModifiedField() & bitMask) != 0L;
  }

  /**
   * Any bits in "bitMask" that are 1 will be set.
   */
  protected void setBits(long bitMask) {
    boolean done;
    do {
      long bits = getLastModifiedField();
      long newBits = bits | bitMask;
      if (bits == newBits) {
        return;
      }
      done = compareAndSetLastModifiedField(bits, newBits);
    } while (!done);
  }

  /**
   * Any bits in "bitMask" that are 0 will be cleared.
   */
  protected void clearBits(long bitMask) {
    boolean done;
    do {
      long bits = getLastModifiedField();
      long newBits = bits & bitMask;
      if (bits == newBits) {
        return;
      }
      done = compareAndSetLastModifiedField(bits, newBits);
    } while (!done);
  }

  @Override
  @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE)
  public Object prepareValueForCache(RegionEntryContext r,
      @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE) Object val, boolean isEntryUpdate) {
    return prepareValueForCache(r, val, null, isEntryUpdate);
  }

  @Override
  @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE)
  public Object prepareValueForCache(RegionEntryContext r,
      @Retained(ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE) Object val, EntryEventImpl event,
      boolean isEntryUpdate) {
    if (r != null && r.getOffHeap() && okToStoreOffHeap(val, this)) {
      if (val instanceof StoredObject) {
        // Check to see if val has the same compression settings as this region.
        // The recursive calls in this section are safe because
        // we only do it after copy the off-heap value to the heap.
        // This is needed to fix bug 52057.
        StoredObject soVal = (StoredObject) val;
        assert !soVal.isCompressed();
        if (r.getCompressor() != null) {
          // val is uncompressed and we need a compressed value.
          // So copy the off-heap value to the heap in a form that can be compressed.
          byte[] valAsBytes = soVal.getValueAsHeapByteArray();
          Object heapValue;
          if (soVal.isSerialized()) {
            heapValue = CachedDeserializableFactory.create(valAsBytes, r.getCache());
          } else {
            heapValue = valAsBytes;
          }
          return prepareValueForCache(r, heapValue, event, isEntryUpdate);
        }
        if (soVal.hasRefCount()) {
          // if the reused StoredObject has a refcount then need to increment it
          if (!soVal.retain()) {
            throw new IllegalStateException("Could not use an off heap value because it was freed");
          }
        }
        // else it is has no refCount so just return it as prepared.
      } else {
        byte[] data;
        boolean isSerialized = !(val instanceof byte[]);
        if (isSerialized) {
          if (event != null && event.getCachedSerializedNewValue() != null) {
            data = event.getCachedSerializedNewValue();
          } else {
            if (val instanceof CachedDeserializable) {
              data = ((CachedDeserializable) val).getSerializedValue();
            } else if (val instanceof PdxInstance) {
              try {
                data = ((ConvertableToBytes) val).toBytes();
              } catch (IOException e) {
                throw new PdxSerializationException("Could not convert " + val + " to bytes", e);
              }
            } else {
              data = EntryEventImpl.serialize(val);
            }
            if (event != null) {
              event.setCachedSerializedNewValue(data);
            }
          }
        } else {
          data = (byte[]) val;
        }
        byte[] compressedData = compressBytes(r, data);
        // TODO: array comparison is broken
        boolean isCompressed = compressedData != data;
        ReferenceCountHelper.setReferenceCountOwner(this);
        MemoryAllocator ma = MemoryAllocatorImpl.getAllocator(); // fix for bug 47875
        val = ma.allocateAndInitialize(compressedData, isSerialized, isCompressed, data);
        ReferenceCountHelper.setReferenceCountOwner(null);
      }
      return val;
    }
    @Unretained
    Object nv = val;
    if (nv instanceof StoredObject) {
      // This off heap value is being put into a on heap region.
      byte[] data = ((StoredObject) nv).getSerializedValue();
      nv = CachedDeserializableFactory.create(data, r.getCache());
    }
    if (nv instanceof PdxInstanceImpl) {
      // We do not want to put PDXs in the cache as values.
      // So get the serialized bytes and use a CachedDeserializable.
      try {
        byte[] data = ((ConvertableToBytes) nv).toBytes();
        byte[] compressedData = compressBytes(r, data);
        // TODO: array comparison is broken
        if (data == compressedData) {
          nv = CachedDeserializableFactory.create(data, r.getCache());
        } else {
          nv = compressedData;
        }
      } catch (IOException e) {
        throw new PdxSerializationException("Could not convert " + nv + " to bytes", e);
      }
    } else {
      nv = compress(r, nv, event);
    }
    return nv;
  }

  @Override
  @Unretained
  public Object getValue() {
    return getValueField();
  }

  @Override
  public boolean isUpdateInProgress() {
    return areAnyBitsSet(UPDATE_IN_PROGRESS);
  }

  @Override
  public void setUpdateInProgress(final boolean underUpdate) {
    if (underUpdate) {
      setBits(UPDATE_IN_PROGRESS);
    } else {
      clearBits(~UPDATE_IN_PROGRESS);
    }
  }

  @Override
  public boolean isCacheListenerInvocationInProgress() {
    return areAnyBitsSet(LISTENER_INVOCATION_IN_PROGRESS);
  }

  @Override
  public void setCacheListenerInvocationInProgress(final boolean isListenerInvoked) {
    if (isListenerInvoked) {
      setBits(LISTENER_INVOCATION_IN_PROGRESS);
    } else {
      clearBits(~LISTENER_INVOCATION_IN_PROGRESS);
    }
  }

  @Override
  public synchronized boolean isInUseByTransaction() {
    return areAnyBitsSet(IN_USE_BY_TX);
  }

  private void setInUseByTransaction(final boolean v) {
    if (v) {
      setBits(IN_USE_BY_TX);
    } else {
      clearBits(~IN_USE_BY_TX);
    }
  }

  @Override
  public synchronized void incRefCount() {
    TXManagerImpl.incRefCount(this);
    setInUseByTransaction(true);
  }

  @Override
  public synchronized void decRefCount(EvictionList evictionList, InternalRegion region) {
    if (TXManagerImpl.decRefCount(this)) {
      if (isInUseByTransaction()) {
        setInUseByTransaction(false);
        if (!isDestroyedOrRemoved()) {
          appendToEvictionList(evictionList);
          if (region != null && region.isEntryExpiryPossible()) {
            region.addExpiryTaskIfAbsent(this);
          }
        }
      }
    }
  }

  @Override
  public synchronized void resetRefCount(EvictionList evictionList) {
    if (isInUseByTransaction()) {
      setInUseByTransaction(false);
      appendToEvictionList(evictionList);
    }
  }

  protected void appendToEvictionList(EvictionList evictionList) {
    // nothing
  }

  void _setValue(Object val) {
    setValueField(val);
  }

  @Override
  public Token getValueAsToken() {
    Object v = getValueField();
    if (v == null || v instanceof Token) {
      return (Token) v;
    } else {
      return Token.NOT_A_TOKEN;
    }
  }

  /**
   * Reads the value of this region entry. Provides low level access to the value field.
   *
   * @return possible OFF_HEAP_OBJECT (caller uses region entry reference)
   */
  @Unretained
  protected abstract Object getValueField();

  /**
   * Set the value of this region entry. Provides low level access to the value field.
   *
   * @param v the new value to set
   */
  protected abstract void setValueField(@Unretained Object v);

  @Override
  @Retained
  public Object getTransformedValue() {
    return getValueRetain(null, false);
  }

  @Override
  public boolean getValueWasResultOfSearch() {
    return areAnyBitsSet(VALUE_RESULT_OF_SEARCH);
  }

  @Override
  public void setValueResultOfSearch(boolean v) {
    if (v) {
      setBits(VALUE_RESULT_OF_SEARCH);
    } else {
      clearBits(~VALUE_RESULT_OF_SEARCH);
    }
  }

  public boolean hasValidVersion() {
    VersionStamp stamp = (VersionStamp) this;
    return stamp.getRegionVersion() != 0 || stamp.getEntryVersion() != 0;
  }

  @Override
  public boolean hasStats() {
    // override this in implementations that have stats
    return false;
  }

  @Override
  public Object getMapValue() {
    return this;
  }

  @Override
  public void setMapValue(final Object newValue) {
    if (this != newValue) {
      Assert.fail("AbstractRegionEntry#setMapValue: unexpected setMapValue " + "with newValue="
          + newValue + ", this=" + this);
    }
  }

  protected abstract void setEntryHash(int v);

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append('@')
        .append(Integer.toHexString(System.identityHashCode(this))).append(" (");
    return appendFieldsToString(sb).append(')').toString();
  }

  protected StringBuilder appendFieldsToString(final StringBuilder sb) {
    // OFFHEAP _getValue ok: the current toString on ObjectChunk is safe to use without incing
    // refcount.
    sb.append("key=").append(getKey()).append("; rawValue=").append(getValue());
    VersionStamp stamp = getVersionStamp();
    if (stamp != null) {
      sb.append("; version=").append(stamp.asVersionTag()).append(";member=")
          .append(stamp.getMemberID());
    }
    return sb;
  }

  /**
   * This generates version tags for outgoing messages for all subclasses supporting concurrency
   * versioning. It also sets the entry's version stamp to the tag's values.
   */
  @Override
  public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
      InternalRegion region, EntryEventImpl event) {
    VersionStamp stamp = getVersionStamp();
    if (stamp != null && region.getServerProxy() == null) {
      // clients do not generate versions
      int v = stamp.getEntryVersion() + 1;
      if (v > 0xFFFFFF) {
        v -= 0x1000000; // roll-over
      }
      VersionSource previous = stamp.getMemberID();

      // For non persistent regions, we allow the member to be null and
      // when we send a message and the remote side can determine the member
      // from the sender. For persistent regions, we need to send
      // the persistent id to the remote side.
      //
      // TODO - RVV - optimize the way we send the persistent id to save
      // space.
      if (member == null) {
        VersionSource regionMember = region.getVersionMember();
        if (regionMember instanceof DiskStoreID) {
          member = regionMember;
        }
      }

      VersionTag tag = VersionTag.create(member);
      tag.setEntryVersion(v);
      if (region.getVersionVector() != null) {
        // Use region version if already provided, else generate
        long nextRegionVersion = event.getNextRegionVersion();
        if (nextRegionVersion != -1) {
          // Set on the tag and record it locally
          tag.setRegionVersion(nextRegionVersion);
          RegionVersionVector rvv = region.getVersionVector();
          rvv.recordVersion(rvv.getOwnerId(), nextRegionVersion);
          if (logger.isDebugEnabled()) {
            logger.debug("recorded region version {}; region={}", nextRegionVersion,
                region.getFullPath());
          }
        } else {
          tag.setRegionVersion(region.getVersionVector().getNextVersion());
        }
      }
      if (withDelta) {
        tag.setPreviousMemberID(previous);
      }
      VersionTag remoteTag = event.getVersionTag();
      if (remoteTag != null && remoteTag.isGatewayTag()) {
        // if this event was received from a gateway we use the remote system's
        // timestamp and dsid.
        tag.setVersionTimeStamp(remoteTag.getVersionTimeStamp());
        tag.setDistributedSystemId(remoteTag.getDistributedSystemId());
        tag.setAllowedByResolver(remoteTag.isAllowedByResolver());
      } else {
        long time = region.cacheTimeMillis();
        int dsid = region.getDistributionManager().getDistributedSystemId();
        // a locally generated change should always have a later timestamp than
        // one received from a wan gateway, so fake a timestamp if necessary
        if (time <= stamp.getVersionTimeStamp() && dsid != tag.getDistributedSystemId()) {
          time = stamp.getVersionTimeStamp() + 1;
        }
        tag.setVersionTimeStamp(time);
        tag.setDistributedSystemId(dsid);
      }
      stamp.setVersions(tag);
      stamp.setMemberID(member);
      event.setVersionTag(tag);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "generated tag {}; key={}; oldvalue={} newvalue={} client={} region={}; rvv={}", tag,
            event.getKey(), event.getOldValueStringForm(), event.getNewValueStringForm(),
            event.getContext() == null ? "none"
                : event.getContext().getDistributedMember().getName(),
            region.getFullPath(), region.getVersionVector());
      }
      return tag;
    }
    return null;
  }

  /**
   * This performs a concurrency check.
   *
   * This check compares the version number first, followed by the member ID.
   *
   * Wraparound of the version number is detected and handled by extending the range of versions by
   * one bit.
   *
   * The normal membership ID comparison method is used.
   * <p>
   *
   * Note that a tag from a remote (WAN) system may be in the event. If this is the case this method
   * will either invoke a user plugin that allows/disallows the event (and may modify the value) or
   * it determines whether to allow or disallow the event based on timestamps and
   * distributedSystemIDs.
   *
   * @throws ConcurrentCacheModificationException if the event conflicts with an event that has
   *         already been applied to the entry.
   */
  public void processVersionTag(EntryEvent cacheEvent) {
    processVersionTag(cacheEvent, true);
  }

  protected void processVersionTag(EntryEvent cacheEvent, boolean conflictCheck) {
    EntryEventImpl event = (EntryEventImpl) cacheEvent;
    VersionTag tag = event.getVersionTag();
    if (tag == null) {
      return;
    }

    try {
      if (tag.isGatewayTag()) {
        // this may throw ConcurrentCacheModificationException or modify the event
        if (processGatewayTag(cacheEvent)) {
          return;
        }
        assert false : "processGatewayTag failure - returned false";
      }

      if (!tag.isFromOtherMember()) {
        if (!event.getOperation().isNetSearch()) {
          // except for netsearch, all locally-generated tags can be ignored
          return;
        }
      }

      final InternalDistributedMember originator =
          (InternalDistributedMember) event.getDistributedMember();
      final VersionSource dmId = event.getRegion().getVersionMember();
      InternalRegion r = event.getRegion();
      boolean eventHasDelta = event.getDeltaBytes() != null && event.getRawNewValue() == null;

      VersionStamp stamp = getVersionStamp();
      // bug #46223, an event received from a peer or a server may be from a different
      // distributed system than the last modification made to this entry so we must
      // perform a gateway conflict check
      if (stamp != null && !tag.isAllowedByResolver()) {
        int stampDsId = stamp.getDistributedSystemId();
        int tagDsId = tag.getDistributedSystemId();

        if (stampDsId != 0 && stampDsId != tagDsId && stampDsId != -1) {
          StringBuilder verbose = null;
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
            verbose = new StringBuilder();
            verbose.append("processing tag for key ").append(getKey()).append(", stamp=")
                .append(stamp.asVersionTag()).append(", tag=").append(tag);
          }
          long stampTime = stamp.getVersionTimeStamp();
          long tagTime = tag.getVersionTimeStamp();
          if (stampTime > 0 && (tagTime > stampTime || (tagTime == stampTime
              && tag.getDistributedSystemId() >= stamp.getDistributedSystemId()))) {
            if (verbose != null) {
              verbose.append(" - allowing event");
              logger.trace(LogMarker.TOMBSTONE_VERBOSE, verbose);
            }
            // Update the stamp with event's version information.
            applyVersionTag(r, stamp, tag, originator);
            return;
          }

          if (stampTime > 0) {
            if (verbose != null) {
              verbose.append(" - disallowing event");
              logger.trace(LogMarker.TOMBSTONE_VERBOSE, verbose);
            }
            r.getCachePerfStats().incConflatedEventsCount();
            persistConflictingTag(r, tag);
            throw new ConcurrentCacheModificationException("conflicting event detected");
          }
        }
      }

      if (r.getVersionVector() != null && r.getServerProxy() == null
          && (r.getDataPolicy().withPersistence() || !r.getScope().isLocal())) {
        // bug #45258 - perf degradation for local regions and RVV
        VersionSource who = tag.getMemberID();
        if (who == null) {
          who = originator;
        }
        r.getVersionVector().recordVersion(who, tag);
      }

      assert !tag.isFromOtherMember()
          || tag.getMemberID() != null : "remote tag is missing memberID";

      // for a long time I had conflict checks turned off in clients when
      // receiving a response from a server and applying it to the cache. This lowered
      // the CPU cost of versioning but eventually had to be pulled for bug #45453

      // events coming from servers while a local sync is held on the entry
      // do not require a conflict check. Conflict checks were already
      // performed on the server and here we just consume whatever was sent back.
      // Event.isFromServer() returns true for client-update messages and
      // for putAll/getAll, which do not hold syncs during the server operation.

      // for a very long time we had conflict checks turned off for PR buckets.
      // Bug 45669 showed a primary dying in the middle of distribution. This caused
      // one backup bucket to have a v2. The other bucket was promoted to primary and
      // generated a conflicting v2. We need to do the check so that if this second
      // v2 loses to the original one in the delta-GII operation that the original v2
      // will be the winner in both buckets.

      // The new value in event is not from GII, even it could be tombstone
      basicProcessVersionTag(r, tag, false, eventHasDelta, dmId, originator, conflictCheck);
    } catch (ConcurrentCacheModificationException ex) {
      event.isConcurrencyConflict(true);
      throw ex;
    }
  }

  protected void basicProcessVersionTag(InternalRegion region, VersionTag tag,
      boolean isTombstoneFromGII, boolean deltaCheck, VersionSource dmId,
      InternalDistributedMember sender, boolean checkForConflict) {

    if (tag != null) {
      VersionStamp stamp = getVersionStamp();

      StringBuilder verbose = null;
      if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
        VersionTag stampTag = stamp.asVersionTag();
        if (stampTag.hasValidVersion() && checkForConflict) {
          // only be verbose here if there's a possibility we might reject the operation
          verbose = new StringBuilder();
          verbose.append("processing tag for key ").append(getKey()).append(", stamp=")
              .append(stamp.asVersionTag()).append(", tag=").append(tag)
              .append(", checkForConflict=").append(checkForConflict);
        }
      }

      if (stamp == null) {
        throw new IllegalStateException(
            "message contained a version tag but this region has no version storage");
      }

      boolean apply = true;

      try {
        if (checkForConflict) {
          apply = checkForConflict(region, stamp, tag, isTombstoneFromGII, deltaCheck, dmId, sender,
              verbose);
        }
      } catch (ConcurrentCacheModificationException e) {
        // Even if we don't apply the operation we should always retain the
        // highest timestamp in order for WAN conflict checks to work correctly
        // because the operation may have been sent to other systems and been
        // applied there
        if (!tag.isGatewayTag() && stamp.getDistributedSystemId() == tag.getDistributedSystemId()
            && tag.getVersionTimeStamp() > stamp.getVersionTimeStamp()) {
          stamp.setVersionTimeStamp(tag.getVersionTimeStamp());
          tag.setTimeStampApplied(true);
          if (verbose != null) {
            verbose
                .append("\nThough in conflict the tag timestamp was more recent and was recorded.");
          }
        }
        throw e;
      } finally {
        if (verbose != null) {
          logger.trace(LogMarker.TOMBSTONE_VERBOSE, verbose);
        }
      }

      if (apply) {
        applyVersionTag(region, stamp, tag, sender);
      }
    }
  }

  private void applyVersionTag(InternalRegion region, VersionStamp stamp, VersionTag tag,
      InternalDistributedMember sender) {
    VersionSource mbr = tag.getMemberID();
    if (mbr == null) {
      mbr = sender;
    }
    mbr = region.getVersionVector().getCanonicalId(mbr);
    tag.setMemberID(mbr);
    stamp.setVersions(tag);
    if (tag.hasPreviousMemberID()) {
      if (tag.getPreviousMemberID() == null) {
        tag.setPreviousMemberID(stamp.getMemberID());
      } else {
        tag.setPreviousMemberID(
            region.getVersionVector().getCanonicalId(tag.getPreviousMemberID()));
      }
    }
  }

  /** perform conflict checking for a stamp/tag */
  private boolean checkForConflict(InternalRegion region, VersionStamp stamp, VersionTag tag,
      boolean isTombstoneFromGII, boolean deltaCheck, VersionSource dmId,
      InternalDistributedMember sender, StringBuilder verbose) {

    int stampVersion = stamp.getEntryVersion();
    int tagVersion = tag.getEntryVersion();

    if (stamp.getVersionTimeStamp() != 0) { // new entries have no timestamp
      // check for wrap-around on the version number
      long difference = tagVersion - stampVersion;
      if (0x10000 < difference || difference < -0x10000) {
        if (verbose != null) {
          verbose.append("\nversion rollover detected: tag=").append(tagVersion).append(" stamp=")
              .append(stampVersion);
        }
        if (difference < 0) {
          tagVersion += 0x1000000;
        } else {
          stampVersion += 0x1000000;
        }
      }
    }
    if (verbose != null) {
      verbose.append("\nstamp=v").append(stampVersion).append(" tag=v").append(tagVersion);
    }

    if (deltaCheck) {
      checkForDeltaConflict(region, stampVersion, tagVersion, stamp, tag, dmId, sender, verbose);
    }

    boolean throwex = false;
    boolean apply = false;
    if (stampVersion == 0 || stampVersion < tagVersion) {
      if (verbose != null) {
        verbose.append(" - applying change");
      }
      apply = true;
    } else if (stampVersion > tagVersion) {
      if (overwritingOldTombstone(region, stamp, tag, verbose)
          && tag.getVersionTimeStamp() > stamp.getVersionTimeStamp()) {
        apply = true;
      } else {
        // check for an incoming expired tombstone from an initial image chunk.
        if (tagVersion > 0
            && isExpiredTombstone(region, tag.getVersionTimeStamp(), isTombstoneFromGII)
            && tag.getVersionTimeStamp() > stamp.getVersionTimeStamp()) {
          // A special case to apply: when remote entry is expired tombstone, then let local vs
          // remote with newer timestamp to win
          if (verbose != null) {
            verbose.append(" - applying change in Delta GII");
          }
          apply = true;
        } else {
          if (verbose != null) {
            verbose.append(" - disallowing");
          }
          throwex = true;
        }
      }
    } else {
      if (overwritingOldTombstone(region, stamp, tag, verbose)) {
        apply = true;
      } else {
        // compare member IDs
        VersionSource stampID = stamp.getMemberID();
        if (stampID == null) {
          stampID = dmId;
        }
        VersionSource tagID = tag.getMemberID();
        if (tagID == null) {
          tagID = sender;
        }
        if (verbose != null) {
          verbose.append("\ncomparing IDs");
        }
        int compare = stampID.compareTo(tagID);
        if (compare < 0) {
          if (verbose != null) {
            verbose.append(" - applying change");
          }
          apply = true;
        } else if (compare > 0) {
          if (verbose != null) {
            verbose.append(" - disallowing");
          }
          throwex = true;
        } else if (tag.isPosDup()) {
          if (verbose != null) {
            verbose.append(" - disallowing duplicate marked with posdup");
          }
          throwex = true;
        } else {
          if (verbose != null) {
            verbose.append(" - allowing duplicate");
          }
        }
      }
    }

    if (!apply && throwex) {
      region.getCachePerfStats().incConflatedEventsCount();
      persistConflictingTag(region, tag);
      throw new ConcurrentCacheModificationException();
    }

    return apply;
  }

  private boolean isExpiredTombstone(InternalRegion region, long timestamp, boolean isTombstone) {
    return isTombstone
        && timestamp + TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT <= region.cacheTimeMillis();
  }

  private boolean overwritingOldTombstone(InternalRegion region, VersionStamp stamp, VersionTag tag,
      StringBuilder verbose) {
    // Tombstone GC does not use locking to stop operations when old tombstones
    // are being removed. Because of this we might get an operation that was applied
    // in another VM that has just reaped a tombstone and is now using a reset
    // entry version number. Because of this we check the timestamp on the current
    // local entry and see if it is old enough to have expired. If this is the case
    // we accept the change and allow the tag to be recorded
    long stampTime = stamp.getVersionTimeStamp();
    if (isExpiredTombstone(region, stampTime, isTombstone())) {
      // no local change since the tombstone would have timed out - accept the change
      if (verbose != null) {
        verbose.append(" - accepting because local timestamp is old");
      }
      return true;
    } else {
      return false;
    }
  }

  protected void persistConflictingTag(InternalRegion region, VersionTag tag) {
    // only persist region needs to persist conflict tag
  }

  /**
   * for an event containing a delta we must check to see if the tag's previous member id is the
   * stamp's member id and ensure that the version is only incremented by 1. Otherwise the delta is
   * being applied to a value that does not match the source of the delta.
   */
  private void checkForDeltaConflict(InternalRegion region, long stampVersion, long tagVersion,
      VersionStamp stamp, VersionTag tag, VersionSource dmId, InternalDistributedMember sender,
      StringBuilder verbose) {

    if (tagVersion != stampVersion + 1) {
      if (verbose != null) {
        verbose.append("\ndelta requires full value due to version mismatch");
      }
      region.getCachePerfStats().incDeltaFailedUpdates();
      throw new InvalidDeltaException("delta cannot be applied due to version mismatch");

    } else {
      // make sure the tag was based on the value in this entry by checking the
      // tag's previous-changer ID against this stamp's current ID
      VersionSource stampID = stamp.getMemberID();
      if (stampID == null) {
        stampID = dmId;
      }
      VersionSource tagID = tag.getPreviousMemberID();
      if (tagID == null) {
        tagID = sender;
      }
      if (!tagID.equals(stampID)) {
        if (verbose != null) {
          verbose.append("\ndelta requires full value.  tag.previous=").append(tagID)
              .append(" but stamp.current=").append(stampID);
        }
        region.getCachePerfStats().incDeltaFailedUpdates();
        throw new InvalidDeltaException("delta cannot be applied due to version ID mismatch");
      }
    }
  }

  private boolean processGatewayTag(EntryEvent cacheEvent) {
    // Gateway tags are installed in the server-side InternalRegion cache
    // modification methods. They do not have version numbers or distributed
    // member IDs. Instead they only have timestamps and distributed system IDs.

    // If there is a resolver plug-in, invoke it. Otherwise we use the timestamps and
    // distributed system IDs to determine whether to allow the event to proceed.

    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (isRemoved() && !isTombstone()) {
      return true; // no conflict on a new entry
    }
    EntryEventImpl event = (EntryEventImpl) cacheEvent;
    VersionTag tag = event.getVersionTag();
    long stampTime = getVersionStamp().getVersionTimeStamp();
    long tagTime = tag.getVersionTimeStamp();
    int stampDsid = getVersionStamp().getDistributedSystemId();
    int tagDsid = tag.getDistributedSystemId();
    if (isDebugEnabled) {
      logger.debug(
          "processing gateway version information for {}.  Stamp dsid={} time={} Tag dsid={} time={}",
          event.getKey(), stampDsid, stampTime, tagDsid, tagTime);
    }
    if (tagTime == VersionTag.ILLEGAL_VERSION_TIMESTAMP) {
      return true; // no timestamp received from other system - just apply it
    }
    // According to GatewayConflictResolver's java doc, it will only be used on tag with different
    // distributed system id than stamp's
    if (stampDsid == -1) {
      return true;
    } else if (tagDsid == stampDsid) {
      if (tagTime >= stampTime) {
        return true;
      } else {
        throw new ConcurrentCacheModificationException("conflicting WAN event detected");
      }
    }
    GatewayConflictResolver resolver = event.getRegion().getCache().getGatewayConflictResolver();
    if (resolver != null) {
      if (isDebugEnabled) {
        logger.debug("invoking gateway conflict resolver");
      }
      final boolean[] disallow = new boolean[1];
      final Object[] newValue = new Object[] {this};
      GatewayConflictHelper helper = new GatewayConflictHelper() {
        @Override
        public void disallowEvent() {
          disallow[0] = true;
        }

        @Override
        public void changeEventValue(Object value) {
          newValue[0] = value;
        }
      };

      @Released
      TimestampedEntryEventImpl timestampedEvent = (TimestampedEntryEventImpl) event
          .getTimestampedEvent(tagDsid, stampDsid, tagTime, stampTime);

      // gateway conflict resolvers will usually want to see the old value
      if (!timestampedEvent.hasOldValue() && isRemoved()) {
        // OFFHEAP: since isRemoved I think getValue will never be stored off heap in this case
        timestampedEvent.setOldValue(getValue(timestampedEvent.getRegion()));
      }

      Throwable thr = null;
      try {
        resolver.onEvent(timestampedEvent, helper);
      } catch (CancelException cancelled) {
        throw cancelled;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error("Exception occurred in GatewayConflictResolver", t);
        thr = t;
      } finally {
        timestampedEvent.release();
      }

      if (isDebugEnabled) {
        logger.debug("done invoking resolver", thr);
      }
      if (thr == null) {
        if (disallow[0]) {
          if (isDebugEnabled) {
            logger.debug("conflict resolver rejected the event for {}", event.getKey());
          }
          throw new ConcurrentCacheModificationException(
              "WAN conflict resolver rejected the operation");
        }

        tag.setAllowedByResolver(true);

        if (newValue[0] != this) {
          if (isDebugEnabled) {
            logger.debug("conflict resolver changed the value of the event for {}", event.getKey());
          }
          // the resolver changed the event value!
          event.setNewValue(newValue[0]);
        }
        // if nothing was done then we allow the event
        if (isDebugEnabled) {
          logger.debug("change was allowed by conflict resolver: {}", tag);
        }
        return true;
      }
    }
    if (isDebugEnabled) {
      logger.debug("performing normal WAN conflict check");
    }
    if (tagTime > stampTime || tagTime == stampTime && tagDsid >= stampDsid) {
      if (isDebugEnabled) {
        logger.debug("allowing event");
      }
      return true;
    }
    if (isDebugEnabled) {
      logger.debug("disallowing event for {}", event.getKey());
    }
    throw new ConcurrentCacheModificationException("conflicting WAN event detected");
  }

  static boolean isCompressible(RegionEntryContext context, Object value) {
    return value != null && context != null && context.getCompressor() != null
        && !Token.isInvalidOrRemoved(value);
  }

  /* subclasses supporting versions must override this */
  @Override
  public VersionStamp getVersionStamp() {
    return null;
  }

  @Override
  public boolean isValueNull() {
    return null == getValueAsToken();
  }

  @Override
  public boolean isInvalid() {
    return Token.isInvalid(getValueAsToken());
  }

  @Override
  public boolean isDestroyed() {
    return Token.isDestroyed(getValueAsToken());
  }

  @Override
  public void setValueToNull() {
    _setValue(null);
  }

  @Override
  public boolean isInvalidOrRemoved() {
    return Token.isInvalidOrRemoved(getValueAsToken());
  }

  /**
   * This is only retained in off-heap subclasses. However, it's marked as Retained here so that
   * callers are aware that the value may be retained.
   */
  @Override
  @Retained
  public Object getValueRetain(RegionEntryContext context, boolean decompress) {
    if (decompress) {
      return decompress(context, getValue());
    } else {
      return getValue();
    }
  }

  @Override
  public void returnToPool() {
    // noop by default
  }

  @Override
  public boolean isEvicted() {
    return false;
  }
}
