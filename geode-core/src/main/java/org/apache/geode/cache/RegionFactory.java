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
package org.apache.geode.cache;

import java.io.File;
import java.util.Properties;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.compression.Compressor;
import org.apache.geode.distributed.LeaseExpiredException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;

/**
 * {@code RegionFactory} is used to create {@link Region regions} in a {@link Cache cache}.
 * Instances of this interface can be created:
 * <ul>
 * <li>using a {@link RegionShortcut shortcut} by calling
 * {@link Cache#createRegionFactory(RegionShortcut)} which will initialize the factory with the
 * shortcut's region attributes
 * <li>using a named region attribute by calling {@link Cache#createRegionFactory(String)} which
 * will initialize the factory the named region attributes
 * <li>using a region attribute instance by calling
 * {@link Cache#createRegionFactory(RegionAttributes)} which will initialize the factory with the
 * given region attributes
 * <li>by calling {@link Cache#createRegionFactory()} which will initialize the factory with
 * defaults
 * </ul>
 * Once the factory has been created it can be customized with its setter methods.
 * <p>
 * The final step is to produce a {@link Region} by calling {@link #create(String)}.
 * <p>
 * Example: Create a replicate region with a CacheListener
 *
 * <PRE>
 * Cache c = new CacheFactory().create();
 * // Create replicate region.
 * // Add a cache listener before creating region
 * Region r = c.createRegionFactory(REPLICATE).addCacheListener(myListener).create("replicate");
 * </PRE>
 * <p>
 * Example: Create a partition region that has redundancy
 *
 * <PRE>
 * Cache c = new CacheFactory().create();
 * // Create replicate region.
 * // Add a cache listener before creating region
 * Region r = c.createRegionFactory(PARTITION_REDUNDANT).create("partition");
 * </PRE>
 *
 * @since GemFire 5.0
 */
public class RegionFactory<K, V> {
  private final AttributesFactory<K, V> attrsFactory;
  private final InternalCache cache;

  /**
   * For internal use only.
   *
   * @since GemFire 6.5
   */
  protected RegionFactory(InternalCache cache) {
    this.cache = cache;
    attrsFactory = new AttributesFactory<>();
  }

  /**
   * For internal use only.
   *
   * @since GemFire 6.5
   */
  protected RegionFactory(InternalCache cache, RegionShortcut pra) {
    this.cache = cache;
    RegionAttributes ra = cache.getRegionAttributes(pra.toString());
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + pra + " has been removed.");
    }
    attrsFactory = new AttributesFactory<>(ra);
  }

  /**
   * For internal use only.
   *
   * @since GemFire 6.5
   */
  protected RegionFactory(InternalCache cache, RegionAttributes ra) {
    this.cache = cache;
    attrsFactory = new AttributesFactory<>(ra);
  }

  /**
   * Constructs a RegionFactory that is a copy of an existing RegionFactory
   *
   * @since Geode 1.12.0
   */
  @VisibleForTesting
  protected RegionFactory(RegionFactory<K, V> regionFactory) {
    attrsFactory = new AttributesFactory<>(regionFactory.getRegionAttributes());
    cache = regionFactory.getCache();
  }

  /**
   * For internal use only.
   *
   * @since GemFire 6.5
   */
  protected RegionFactory(InternalCache cache, String regionAttributesId) {
    this.cache = cache;
    RegionAttributes<K, V> ra = getCache().getRegionAttributes(regionAttributesId);
    if (ra == null) {
      throw new IllegalStateException(String.format("No attributes associated with %s",
          regionAttributesId));
    }
    attrsFactory = new AttributesFactory<>(ra);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If no DistributedSystem
   * exists it creates a DistributedSystem with default configuration, otherwise it uses the
   * existing DistributedSystem. The default Region configuration is used.
   *
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link Cache#createRegionFactory()} instead.
   */
  @Deprecated
  public RegionFactory() throws CacheWriterException, RegionExistsException, TimeoutException {
    this((InternalCache) new CacheFactory().create());
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If no DistributedSystem
   * exists it creates a DistributedSystem with default configuration, otherwise it uses the
   * existing DistributedSystem. The Region configuration is initialized using the provided
   * RegionAttributes.
   *
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link Cache#createRegionFactory(RegionAttributes)} instead.
   */
  @Deprecated
  public RegionFactory(RegionAttributes<K, V> regionAttributes)
      throws CacheWriterException, RegionExistsException, TimeoutException {
    this((InternalCache) new CacheFactory().create(), regionAttributes);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If no DistributedSystem
   * exists it creates a DistributedSystem with default configuration, otherwise it uses the
   * existing DistributedSystem. The Region configuration is initialized using the RegionAttributes
   * identified in the cache.xml file by the provided identifier.
   *
   * @param regionAttributesId that identifies a set of RegionAttributes in the cache-xml file.
   * @see Cache#getRegionAttributes
   * @throws IllegalArgumentException if there are no attributes associated with the id
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link Cache#createRegionFactory(String)} instead.
   */
  @Deprecated
  public RegionFactory(String regionAttributesId)
      throws CacheWriterException, RegionExistsException, TimeoutException {
    this((InternalCache) new CacheFactory().create(), regionAttributesId);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If a DistributedSystem
   * already exists with the same properties it uses that DistributedSystem, otherwise a
   * DistributedSystem is created using the provided properties. The default Region configuration is
   * used.
   *
   * @param distributedSystemProperties an instance of Properties containing
   *        {@code DistributedSystem} configuration
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} and
   *             {@link Cache#createRegionFactory()} instead.
   */
  @Deprecated
  public RegionFactory(Properties distributedSystemProperties)
      throws CacheWriterException, RegionExistsException, TimeoutException {
    this((InternalCache) new CacheFactory(distributedSystemProperties).create());
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If a DistributedSystem
   * already exists with the same properties it uses that DistributedSystem, otherwise a
   * DistributedSystem is created using the provided properties. The initial Region configuration is
   * set using the RegionAttributes provided.
   *
   * @param distributedSystemProperties properties used to either find or create a
   *        DistributedSystem.
   * @param regionAttributes the initial Region configuration for this RegionFactory.
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} and
   *             {@link Cache#createRegionFactory(RegionAttributes)} instead.
   */
  @Deprecated
  public RegionFactory(Properties distributedSystemProperties,
      RegionAttributes<K, V> regionAttributes)
      throws CacheWriterException, RegionExistsException, TimeoutException {
    this((InternalCache) new CacheFactory(distributedSystemProperties).create(), regionAttributes);
  }

  /**
   * Constructs a RegionFactory by creating a DistributedSystem and a Cache. If a DistributedSystem
   * already exists whose properties match those provied, it uses that DistributedSystem. The Region
   * configuration is initialized using the RegionAttributes identified in the cache.xml file by the
   * provided identifier.
   *
   * @param distributedSystemProperties properties used to either find or create a
   *        DistributedSystem.
   * @param regionAttributesId the identifier for set of RegionAttributes in the cache.xml file used
   *        as the initial Region configuration for this RegionFactory.
   * @throws IllegalArgumentException if there are no attributes associated with the id
   *
   * @throws CacheException if unable to connect the DistributedSystem or create a Cache
   * @deprecated as of 6.5 use {@link CacheFactory#CacheFactory(Properties)} and
   *             {@link Cache#createRegionFactory(String)} instead.
   *
   */
  @Deprecated
  public RegionFactory(Properties distributedSystemProperties, String regionAttributesId)
      throws CacheWriterException, RegionExistsException, TimeoutException {
    this((InternalCache) new CacheFactory(distributedSystemProperties).create(),
        regionAttributesId);
  }


  /**
   * Returns the cache used by this factory.
   */
  protected synchronized InternalCache getCache() {
    return cache;
  }

  /**
   * Returns the attributes used by this factory to create a region.
   */
  protected RegionAttributes<K, V> getRegionAttributes() {
    return attrsFactory.create();
  }

  /**
   * Sets the cache loader for the next {@code RegionAttributes} created.
   *
   * @param cacheLoader the cache loader or null if no loader
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setCacheLoader
   *
   */
  public RegionFactory<K, V> setCacheLoader(CacheLoader<K, V> cacheLoader) {
    attrsFactory.setCacheLoader(cacheLoader);
    return this;
  }

  /**
   * Sets the cache writer for the next {@code RegionAttributes} created.
   *
   * @param cacheWriter the cache writer or null if no cache writer
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setCacheWriter
   */
  public RegionFactory<K, V> setCacheWriter(CacheWriter<K, V> cacheWriter) {
    attrsFactory.setCacheWriter(cacheWriter);
    return this;
  }

  /**
   * Adds a cache listener to the end of the list of cache listeners on this factory.
   *
   * @param aListener the cache listener to add
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if {@code aListener} is null
   * @see AttributesFactory#addCacheListener
   */
  public RegionFactory<K, V> addCacheListener(CacheListener<K, V> aListener) {
    attrsFactory.addCacheListener(aListener);
    return this;
  }

  /**
   * Removes all cache listeners and then adds each listener in the specified array. for the next
   * {@code RegionAttributes} created.
   *
   * @param newListeners a possibly null or empty array of listeners to add to this factory.
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if the {@code newListeners} array has a null element
   * @see AttributesFactory#initCacheListeners
   */
  public RegionFactory<K, V> initCacheListeners(CacheListener<K, V>[] newListeners) {
    attrsFactory.initCacheListeners(newListeners);
    return this;
  }

  /**
   * Sets the eviction attributes that controls growth of the Region to be created.
   *
   * @param evictionAttributes for the Region to create
   * @return a reference to this RegionFactory object
   */
  public RegionFactory<K, V> setEvictionAttributes(EvictionAttributes evictionAttributes) {
    attrsFactory.setEvictionAttributes(evictionAttributes);
    return this;
  }

  /**
   * Sets the idleTimeout expiration attributes for region entries for the next
   * {@code RegionAttributes} created. Note that the XML element that corresponds to this method
   * "entry-idle-time", does not include "out" in its name.
   *
   * @param idleTimeout the idleTimeout ExpirationAttributes for entries in this region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if idleTimeout is null
   * @see AttributesFactory#setEntryIdleTimeout
   */
  public RegionFactory<K, V> setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    attrsFactory.setEntryIdleTimeout(idleTimeout);
    return this;
  }

  /**
   * Sets the custom idleTimeout for the next {@code RegionAttributes} created.
   *
   * @param custom the custom method
   * @return the receiver
   * @see AttributesFactory#setCustomEntryIdleTimeout(CustomExpiry)
   */
  public RegionFactory<K, V> setCustomEntryIdleTimeout(CustomExpiry<K, V> custom) {
    attrsFactory.setCustomEntryIdleTimeout(custom);
    return this;
  }

  /**
   * Sets the timeToLive expiration attributes for region entries for the next
   * {@code RegionAttributes} created.
   *
   * @param timeToLive the timeToLive ExpirationAttributes for entries in this region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if timeToLive is null
   * @see AttributesFactory#setEntryTimeToLive
   */
  public RegionFactory<K, V> setEntryTimeToLive(ExpirationAttributes timeToLive) {
    attrsFactory.setEntryTimeToLive(timeToLive);
    return this;
  }

  /**
   * Sets the custom timeToLive expiration method for the next {@code RegionAttributes} created.
   *
   * @param custom the custom method
   * @return the receiver
   * @see AttributesFactory#setCustomEntryTimeToLive(CustomExpiry)
   */
  public RegionFactory<K, V> setCustomEntryTimeToLive(CustomExpiry<K, V> custom) {
    attrsFactory.setCustomEntryTimeToLive(custom);
    return this;
  }

  /**
   * Sets the idleTimeout expiration attributes for the region itself for the next
   * {@code RegionAttributes} created. Note that the XML element that corresponds to this method
   * "region-idle-time", does not include "out" in its name.
   *
   * @param idleTimeout the ExpirationAttributes for this region idleTimeout
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if idleTimeout is null
   * @see AttributesFactory#setRegionIdleTimeout
   */
  public RegionFactory<K, V> setRegionIdleTimeout(ExpirationAttributes idleTimeout) {
    attrsFactory.setRegionIdleTimeout(idleTimeout);
    return this;
  }

  /**
   * Sets the timeToLive expiration attributes for the region itself for the next
   * {@code RegionAttributes} created.
   *
   * @param timeToLive the ExpirationAttributes for this region timeToLive
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if timeToLive is null
   * @see AttributesFactory#setRegionTimeToLive
   */
  public RegionFactory<K, V> setRegionTimeToLive(ExpirationAttributes timeToLive) {
    attrsFactory.setRegionTimeToLive(timeToLive);
    return this;
  }

  /**
   * Sets the scope for the next {@code RegionAttributes} created.
   *
   * @param scopeType the type of Scope to use for the region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if scopeType is null
   * @see AttributesFactory#setScope
   */
  public RegionFactory<K, V> setScope(Scope scopeType) {
    attrsFactory.setScope(scopeType);
    return this;
  }

  /**
   * Sets the data policy for the next {@code RegionAttributes} created.
   *
   * @param dataPolicy The type of mirroring to use for the region
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if dataPolicy is null
   * @see AttributesFactory#setDataPolicy
   */
  public RegionFactory<K, V> setDataPolicy(DataPolicy dataPolicy) {
    attrsFactory.setDataPolicy(dataPolicy);
    return this;
  }

  /**
   * Sets for this region whether or not acks are sent after an update is processed.
   *
   * @param earlyAck set to true for the acknowledgement to be sent prior to processing the update
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setEarlyAck(boolean)
   * @deprecated As of 6.5 this setting no longer has any effect.
   */
  @Deprecated
  public RegionFactory<K, V> setEarlyAck(boolean earlyAck) {
    attrsFactory.setEarlyAck(earlyAck);
    return this;
  }

  /**
   * Sets whether distributed operations on this region should attempt to use multicast.
   *
   * @since GemFire 5.0
   * @param value true to enable multicast
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setMulticastEnabled(boolean)
   */
  public RegionFactory<K, V> setMulticastEnabled(boolean value) {
    attrsFactory.setMulticastEnabled(value);
    return this;
  }

  /**
   * Sets the pool name attribute. This causes regions that use these attributes to be a client
   * region which communicates with the servers that the connection pool communicates with.
   * <p>
   * If this attribute is set to {@code null} or {@code ""} then the connection pool is disabled
   * causing regions that use these attributes to be communicate with peers instead of servers.
   * <p>
   * The named connection pool must exist on the cache at the time these attributes are used to
   * create a region. See {@link PoolManager#createFactory} for how to create a connection pool.
   *
   * @param poolName the name of the connection pool to use; if {@code null} or {@code ""} then the
   *        connection pool attribute is disabled for regions using these attributes.
   * @return a reference to this RegionFactory object
   * @throws IllegalStateException if a cache loader or cache writer has already been set.
   * @since GemFire 5.7
   */
  public RegionFactory<K, V> setPoolName(String poolName) {
    attrsFactory.setPoolName(poolName);
    return this;
  }

  /**
   * Sets whether or not this region should be considered a publisher.
   *
   * @since GemFire 5.0
   * @deprecated as of 6.5
   */
  @Deprecated
  public void setPublisher(boolean v) {}

  /**
   * Sets whether or not conflation is enabled for sending messages to async peers.
   *
   * @param value true to enable async conflation
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setEnableAsyncConflation(boolean)
   */
  public RegionFactory<K, V> setEnableAsyncConflation(boolean value) {
    attrsFactory.setEnableAsyncConflation(value);
    return this;
  }

  /**
   * Sets whether or not conflation is enabled for sending messages from a cache server to its
   * clients.
   *
   * @param value true to enable subscription conflation
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setEnableSubscriptionConflation(boolean)
   */
  public RegionFactory<K, V> setEnableSubscriptionConflation(boolean value) {
    attrsFactory.setEnableSubscriptionConflation(value);
    return this;
  }

  /**
   * Sets the key constraint for the next {@code RegionAttributes} created. Keys in the region will
   * be constrained to this class (or subclass). Any attempt to store a key of an incompatible type
   * in the region will cause a {@code ClassCastException} to be thrown.
   *
   * @param keyConstraint The Class to constrain the keys to, or null if no constraint
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if {@code keyConstraint} is a class denoting a primitive type
   * @see AttributesFactory#setKeyConstraint
   */
  public RegionFactory<K, V> setKeyConstraint(Class<K> keyConstraint) {
    attrsFactory.setKeyConstraint(keyConstraint);
    return this;
  }

  /**
   * Sets the value constraint for the next {@code RegionAttributes} created. Values in the region
   * will be constrained to this class (or subclass). Any attempt to store a value of an
   * incompatible type in the region will cause a {@code ClassCastException} to be thrown.
   *
   * @param valueConstraint The Class to constrain the values to, or null if no constraint
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if {@code valueConstraint} is a class denoting a primitive
   *         type
   * @see AttributesFactory#setValueConstraint
   */
  public RegionFactory<K, V> setValueConstraint(Class<V> valueConstraint) {
    attrsFactory.setValueConstraint(valueConstraint);
    return this;
  }

  /**
   * Sets the entry initial capacity for the next {@code RegionAttributes} created. This value is
   * used in initializing the map that holds the entries.
   *
   * @param initialCapacity the initial capacity of the entry map
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if initialCapacity is negative.
   * @see java.util.HashMap
   * @see AttributesFactory#setInitialCapacity
   */
  public RegionFactory<K, V> setInitialCapacity(int initialCapacity) {
    attrsFactory.setInitialCapacity(initialCapacity);
    return this;
  }

  /**
   * Sets the entry load factor for the next {@code RegionAttributes} created. This value is used in
   * initializing the map that holds the entries.
   *
   * @param loadFactor the load factor of the entry map
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if loadFactor is nonpositive
   * @see java.util.HashMap
   * @see AttributesFactory#setLoadFactor
   */
  public RegionFactory<K, V> setLoadFactor(float loadFactor) {
    attrsFactory.setLoadFactor(loadFactor);
    return this;
  }

  /**
   * Sets the concurrency level tof the next {@code RegionAttributes} created. This value is used in
   * initializing the map that holds the entries.
   *
   * @param concurrencyLevel the concurrency level of the entry map
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if concurrencyLevel is nonpositive
   * @see AttributesFactory#setConcurrencyLevel
   */
  public RegionFactory<K, V> setConcurrencyLevel(int concurrencyLevel) {
    attrsFactory.setConcurrencyLevel(concurrencyLevel);
    return this;
  }

  /**
   * Enables a versioning system that detects concurrent modifications and ensures that region
   * contents are consistent across the distributed system. This setting must be the same in each
   * member having the region.
   *
   * @since GemFire 7.0
   * @param enabled whether concurrency checks should be enabled for the region
   * @see AttributesFactory#setConcurrencyChecksEnabled
   */
  public RegionFactory<K, V> setConcurrencyChecksEnabled(boolean enabled) {
    attrsFactory.setConcurrencyChecksEnabled(enabled);
    return this;
  }

  /**
   * Returns whether or not disk writes are asynchronous.
   *
   * @return a reference to this RegionFactory object
   * @see Region#writeToDisk
   * @see AttributesFactory#setDiskWriteAttributes
   * @deprecated as of 6.5 use {@link #setDiskStoreName} instead
   *
   */
  @Deprecated
  public RegionFactory<K, V> setDiskWriteAttributes(DiskWriteAttributes attrs) {
    attrsFactory.setDiskWriteAttributes(attrs);
    return this;
  }

  /**
   * Sets the DiskStore name attribute. This causes the region to belong to the DiskStore.
   *
   * @param name the name of the diskstore
   * @return a reference to this RegionFactory object
   * @since GemFire 6.5
   *
   * @see AttributesFactory#setDiskStoreName
   */
  public RegionFactory<K, V> setDiskStoreName(String name) {
    attrsFactory.setDiskStoreName(name);
    return this;
  }

  /**
   * Sets whether or not the writing to the disk is synchronous.
   *
   * @param isSynchronous boolean if true indicates synchronous writes
   * @return a reference to this RegionFactory object
   * @since GemFire 6.5
   */
  public RegionFactory<K, V> setDiskSynchronous(boolean isSynchronous) {
    attrsFactory.setDiskSynchronous(isSynchronous);
    return this;
  }

  /**
   * Sets the directories to which the region's data are written. If multiple directories are used,
   * GemFire will attempt to distribute the data evenly amongst them.
   *
   * @return a reference to this RegionFactory object
   *
   * @see AttributesFactory#setDiskDirs
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setDiskDirs} instead
   */
  @Deprecated
  public RegionFactory<K, V> setDiskDirs(File[] diskDirs) {
    attrsFactory.setDiskDirs(diskDirs);
    return this;
  }

  /**
   * Sets the directories to which the region's data are written and also set their sizes in
   * megabytes
   *
   * @return a reference to this RegionFactory object
   * @throws IllegalArgumentException if length of the size array does not match to the length of
   *         the dir array
   *
   * @since GemFire 5.1
   * @see AttributesFactory#setDiskDirsAndSizes
   * @deprecated as of 6.5 use {@link DiskStoreFactory#setDiskDirsAndSizes} instead
   */
  @Deprecated
  public RegionFactory<K, V> setDiskDirsAndSizes(File[] diskDirs, int[] diskSizes) {
    attrsFactory.setDiskDirsAndSizes(diskDirs, diskSizes);
    return this;
  }

  /**
   * Sets the {@code PartitionAttributes} that describe how the region is partitioned among members
   * of the distributed system.
   *
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setPartitionAttributes
   */
  public RegionFactory<K, V> setPartitionAttributes(PartitionAttributes partition) {
    attrsFactory.setPartitionAttributes(partition);
    return this;
  }

  /**
   * Sets the {@code MembershipAttributes} that describe the membership roles required for reliable
   * access to the region.
   *
   * @param ra the MembershipAttributes to use
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setMembershipAttributes
   * @deprecated this API is scheduled to be removed
   */
  @Deprecated
  public RegionFactory<K, V> setMembershipAttributes(MembershipAttributes ra) {
    attrsFactory.setMembershipAttributes(ra);
    return this;
  }

  /**
   * Sets how indexes on this region are kept current.
   *
   * @param synchronous whether indexes are maintained in a synchronized fashion
   * @return a reference to this RegionFactory object
   */
  public RegionFactory<K, V> setIndexMaintenanceSynchronous(boolean synchronous) {
    attrsFactory.setIndexMaintenanceSynchronous(synchronous);
    return this;
  }

  /**
   * Sets whether statistics are enabled for this region and its entries.
   *
   * @param statisticsEnabled whether statistics are enabled
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setStatisticsEnabled
   */
  public RegionFactory<K, V> setStatisticsEnabled(boolean statisticsEnabled) {
    attrsFactory.setStatisticsEnabled(statisticsEnabled);
    return this;
  }

  /**
   * Sets whether operations on this region should be controlled by JTA transactions or not
   *
   * @since GemFire 5.0
   */
  public RegionFactory<K, V> setIgnoreJTA(boolean flag) {
    attrsFactory.setIgnoreJTA(flag);
    return this;
  }

  /**
   * Sets whether this region should become lock grantor.
   *
   * @param isLockGrantor whether this region should become lock grantor
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setLockGrantor
   */
  public RegionFactory<K, V> setLockGrantor(boolean isLockGrantor) {
    attrsFactory.setLockGrantor(isLockGrantor);
    return this;
  }

  /**
   * Sets the kind of interest this region has in events occuring in other caches that define the
   * region by the same name.
   *
   * @param sa the attributes decribing the interest
   * @return a reference to this RegionFactory object
   * @see AttributesFactory#setSubscriptionAttributes(SubscriptionAttributes)
   */
  public RegionFactory<K, V> setSubscriptionAttributes(SubscriptionAttributes sa) {
    attrsFactory.setSubscriptionAttributes(sa);
    return this;
  }

  /**
   * Creates a region with the given name in this factory's {@link Cache} using the configuration
   * contained in this factory. Validation of the provided attributes may cause exceptions to be
   * thrown if there are problems with the configuration data.
   *
   * @param name the name of the region to create
   *
   * @return the region object
   * @throws LeaseExpiredException if lease expired on distributed lock for Scope.GLOBAL
   * @throws RegionExistsException if a region, shared or unshared, is already in this cache
   * @throws TimeoutException if timed out getting distributed lock for Scope.GLOBAL
   * @throws CacheClosedException if the cache is closed
   * @throws IllegalStateException if the supplied RegionAttributes are incompatible with this
   *         region in another cache in the distributed system (see {@link AttributesFactory} for
   *         compatibility rules)
   */
  @SuppressWarnings("deprecation")
  public Region<K, V> create(String name)
      throws CacheExistsException, RegionExistsException, CacheWriterException, TimeoutException {
    return getCache().createRegion(name, getRegionAttributes());
  }

  /**
   * Creates a sub-region in the {@link Cache} using the configuration contained in this
   * RegionFactory. Validation of the provided attributes may cause exceptions to be thrown if there
   * are problems with the configuration data.
   *
   * @param parent the existing region that will contain the created sub-region
   * @param name the name of the region to create
   *
   * @return the region object
   * @throws RegionExistsException if a region with the given name already exists in this cache
   * @throws RegionDestroyedException if the parent region has been closed or destroyed
   * @throws CacheClosedException if the cache is closed
   * @since GemFire 7.0
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  public Region<K, V> createSubregion(Region<?, ?> parent, String name)
      throws RegionExistsException {
    return ((InternalRegion) parent).createSubregion(name, getRegionAttributes());
  }

  /**
   * Sets cloning on region Note: off-heap regions always behave as if cloning is enabled.
   *
   * @return a reference to this RegionFactory object
   * @since GemFire 6.1
   * @see AttributesFactory#setCloningEnabled
   */
  public RegionFactory<K, V> setCloningEnabled(boolean cloningEnable) {
    attrsFactory.setCloningEnabled(cloningEnable);
    return this;
  }

  /**
   * Adds a gatewaySenderId to the RegionAttributes
   *
   * @return a reference to this RegionFactory object
   * @since GemFire 7.0
   * @see AttributesFactory#addGatewaySenderId(String)
   */
  public RegionFactory<K, V> addGatewaySenderId(String gatewaySenderId) {
    attrsFactory.addGatewaySenderId(gatewaySenderId);
    return this;
  }

  /**
   * Adds a asyncEventQueueId to the RegionAttributes
   *
   * @param asyncEventQueueId id of AsyncEventQueue
   * @return a reference to this RegionFactory instance
   * @since GemFire 7.0
   */
  public RegionFactory<K, V> addAsyncEventQueueId(String asyncEventQueueId) {
    attrsFactory.addAsyncEventQueueId(asyncEventQueueId);
    return this;
  }

  /**
   * Set the compressor to be used by this region for compressing region entry values.
   *
   * @param compressor a compressor
   * @return a reference to this RegionFactory instance
   * @since GemFire 8.0
   */
  public RegionFactory<K, V> setCompressor(Compressor compressor) {
    attrsFactory.setCompressor(compressor);
    return this;
  }

  /**
   * Enables this region's usage of off-heap memory if true.
   *
   * @param offHeap boolean flag to enable off-heap memory
   * @since Geode 1.0
   */
  public RegionFactory<K, V> setOffHeap(boolean offHeap) {
    attrsFactory.setOffHeap(offHeap);
    return this;
  }
}
