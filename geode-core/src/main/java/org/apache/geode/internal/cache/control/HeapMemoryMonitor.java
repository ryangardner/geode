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
package org.apache.geode.internal.cache.control;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.Statistics;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState;
import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import org.apache.geode.internal.cache.execute.AllowExecutionInLowMemory;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.LocalStatListener;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Allows for the setting of eviction and critical thresholds. These thresholds are compared against
 * current heap usage and, with the help of {#link InternalResourceManager}, dispatches events when
 * the thresholds are crossed. Gathering memory usage information from the JVM is done using a
 * listener on the MemoryMXBean, by polling the JVM and as a listener on GemFire Statistics output
 * in order to accommodate differences in the various JVMs.
 *
 * @since Geode 1.0
 */
public class HeapMemoryMonitor implements NotificationListener, MemoryMonitor {
  private static final Logger logger = LogService.getLogger();

  // Allow for an unknown heap pool for VMs we may support in the future.
  private static final String HEAP_POOL =
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "ResourceManager.HEAP_POOL");

  // Property for setting the JVM polling interval (below)
  public static final String POLLER_INTERVAL_PROP =
      GeodeGlossary.GEMFIRE_PREFIX + "heapPollerInterval";

  // Internal for polling the JVM for changes in heap memory usage.
  private static final int POLLER_INTERVAL = Integer.getInteger(POLLER_INTERVAL_PROP, 500);

  // This holds a new event as it transitions from updateStateAndSendEvent(...) to fillInProfile()
  private final ThreadLocal<MemoryEvent> upcomingEvent = new ThreadLocal<>();

  private ScheduledExecutorService pollerExecutor;

  // Listener for heap memory usage as reported by the Cache stats.
  private final LocalStatListener statListener = new LocalHeapStatListener();

  // JVM MXBean used to report changes in heap memory usage
  @Immutable
  private static final MemoryPoolMXBean tenuredMemoryPoolMXBean;
  static {
    MemoryPoolMXBean matchingMemoryPoolMXBean = null;
    for (MemoryPoolMXBean memoryPoolMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
      if (memoryPoolMXBean.isUsageThresholdSupported() && isTenured(memoryPoolMXBean)) {
        matchingMemoryPoolMXBean = memoryPoolMXBean;
        break;
      }
    }

    tenuredMemoryPoolMXBean = matchingMemoryPoolMXBean;

    if (tenuredMemoryPoolMXBean == null) {
      logger.error("No tenured pools found.  Known pools are: {}",
          getAllMemoryPoolNames());
    }
  }

  // Calculated value for the amount of JVM tenured heap memory available.
  private static final long tenuredPoolMaxMemory;
  /*
   * Calculates the max memory for the tenured pool. Works around JDK bug:
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7078465 by getting max memory from runtime
   * and subtracting all other heap pools from it.
   */
  static {
    if (tenuredMemoryPoolMXBean != null && tenuredMemoryPoolMXBean.getUsage().getMax() != -1) {
      tenuredPoolMaxMemory = tenuredMemoryPoolMXBean.getUsage().getMax();
    } else {
      long calculatedMaxMemory = Runtime.getRuntime().maxMemory();
      List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean p : pools) {
        if (p.getType() == MemoryType.HEAP && p.getUsage().getMax() != -1) {
          calculatedMaxMemory -= p.getUsage().getMax();
        }
      }
      tenuredPoolMaxMemory = calculatedMaxMemory;
    }
  }

  private volatile MemoryThresholds thresholds = new MemoryThresholds(tenuredPoolMaxMemory);
  private volatile MemoryEvent mostRecentEvent = new MemoryEvent(ResourceType.HEAP_MEMORY,
      MemoryState.DISABLED, MemoryState.DISABLED, null, 0L, true, thresholds);
  private volatile MemoryState currentState = MemoryState.DISABLED;

  // Set when startMonitoring() and stopMonitoring() are called
  boolean started = false;

  // Set to true when setEvictionThreshold(...) is called.
  private boolean hasEvictionThreshold = false;

  private final InternalResourceManager resourceManager;
  private final ResourceAdvisor resourceAdvisor;
  private final InternalCache cache;
  private final ResourceManagerStats stats;
  private final TenuredHeapConsumptionMonitor tenuredHeapConsumptionMonitor;

  @MutableForTesting
  private static boolean testDisableMemoryUpdates = false;
  @MutableForTesting
  private static long testBytesUsedForThresholdSet = -1;

  /**
   * Determines if the name of the memory pool MXBean provided matches a list of known tenured pool
   * names.
   *
   * Package private for testing.
   * checkTenuredHeapConsumption
   *
   * @param memoryPoolMXBean The memory pool MXBean to check.
   * @return True if the pool name matches a known tenured pool name, false otherwise.
   */
  static boolean isTenured(MemoryPoolMXBean memoryPoolMXBean) {
    if (memoryPoolMXBean.getType() != MemoryType.HEAP) {
      return false;
    }

    String name = memoryPoolMXBean.getName();

    return name.equals("CMS Old Gen") // Sun Concurrent Mark Sweep GC
        || name.equals("PS Old Gen") // Sun Parallel GC
        || name.equals("G1 Old Gen") // Sun G1 GC
        || name.equals("Old Space") // BEA JRockit 1.5, 1.6 GC
        || name.equals("Tenured Gen") // Hitachi 1.5 GC
        || name.equals("Java heap") // IBM 1.5, 1.6 GC
        || name.equals("GenPauseless Old Gen") // azul C4/GPGC collector

        // Allow an unknown pool name to monitor
        || (HEAP_POOL != null && name.equals(HEAP_POOL));
  }

  HeapMemoryMonitor(final InternalResourceManager resourceManager, final InternalCache cache,
      final ResourceManagerStats stats,
      TenuredHeapConsumptionMonitor tenuredHeapConsumptionMonitor) {
    this.resourceManager = resourceManager;
    resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    this.cache = cache;
    this.stats = stats;
    this.tenuredHeapConsumptionMonitor = tenuredHeapConsumptionMonitor;
  }

  /**
   * Returns the tenured pool MXBean or throws an IllegaleStateException if one couldn't be found.
   */
  public static MemoryPoolMXBean getTenuredMemoryPoolMXBean() {
    if (tenuredMemoryPoolMXBean != null) {
      return tenuredMemoryPoolMXBean;
    }

    throw new IllegalStateException(String.format("No tenured pools found.  Known pools are: %s",
        getAllMemoryPoolNames()));
  }

  /**
   * Returns the names of all available memory pools as a single string.
   */
  private static String getAllMemoryPoolNames() {
    StringBuilder builder = new StringBuilder("[");

    for (MemoryPoolMXBean memoryPoolBean : ManagementFactory.getMemoryPoolMXBeans()) {
      builder.append("(Name=").append(memoryPoolBean.getName()).append(";Type=")
          .append(memoryPoolBean.getType()).append(";UsageThresholdSupported=")
          .append(memoryPoolBean.isUsageThresholdSupported()).append("), ");
    }

    if (builder.length() > 1) {
      builder.setLength(builder.length() - 2);
    }
    builder.append("]");

    return builder.toString();
  }

  public void setMemoryStateChangeTolerance(int memoryStateChangeTolerance) {
    thresholds.setMemoryStateChangeTolerance(memoryStateChangeTolerance);
  }

  public int getMemoryStateChangeTolerance() {
    return thresholds.getMemoryStateChangeTolerance();
  }

  /**
   * Monitoring is done using a combination of data from the JVM and statistics collected from the
   * cache. A usage threshold is set on the MemoryMXBean of the JVM to get notifications when the
   * JVM crosses the eviction or critical thresholds. A separate usage collection is done either by
   * setting up a listener on the cache stats or polling of the JVM, depending on whether stats have
   * been enabled. This separate collection is done to return the state of the heap memory back to a
   * normal state when memory has been freed.
   */
  private void startMonitoring() {
    synchronized (this) {
      if (started) {
        return;
      }

      final boolean statListenerStarted = startCacheStatListener();

      if (!statListenerStarted) {
        startMemoryPoolPoller();
      }

      startJVMThresholdListener();

      started = true;
    }
  }

  /**
   * Stops all three mechanisms from monitoring heap usage.
   */
  @Override
  public void stopMonitoring() {
    synchronized (this) {
      if (!started) {
        return;
      }

      // Stop the poller
      resourceManager.stopExecutor(pollerExecutor);

      // Stop the JVM threshold listener
      NotificationEmitter emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
      try {
        emitter.removeNotificationListener(this, null, null);
        if (logger.isDebugEnabled()) {
          logger.debug("Removed Memory MXBean notification listener" + this);
        }
      } catch (ListenerNotFoundException ignore) {
        if (logger.isDebugEnabled()) {
          logger.debug("This instance '{}' was not registered as a Memory MXBean listener", this);
        }
      }

      // Stop the stats listener
      final GemFireStatSampler sampler = cache.getInternalDistributedSystem().getStatSampler();
      if (sampler != null) {
        sampler.removeLocalStatListener(statListener);
      }

      started = false;
    }
  }

  public static Statistics getTenuredPoolStatistics(StatisticsManager statisticsManager) {
    String tenuredPoolName = getTenuredMemoryPoolMXBean().getName();
    String tenuredPoolType = "PoolStats";
    for (Statistics si : statisticsManager.getStatsList()) {
      if (si.getTextId().contains(tenuredPoolName)
          && si.getType().getName().contains(tenuredPoolType)) {
        return si;
      }
    }
    return null;
  }

  /**
   * Start a listener on the cache stats to monitor memory usage.
   *
   * @return True of the listener was correctly started, false otherwise.
   */
  private boolean startCacheStatListener() {
    final GemFireStatSampler sampler = cache.getInternalDistributedSystem().getStatSampler();
    if (sampler == null) {
      return false;
    }

    try {
      sampler.waitForInitialization();
      Statistics si = getTenuredPoolStatistics(
          cache.getInternalDistributedSystem().getStatisticsManager());
      if (si != null) {
        sampler.addLocalStatListener(statListener, si, "currentUsedMemory");
        if (logger.isDebugEnabled()) {
          logger.debug("Registered stat listener for " + si.getTextId());
        }

        return true;
      }
    } catch (InterruptedException iex) {
      Thread.currentThread().interrupt();
      cache.getCancelCriterion().checkCancelInProgress(iex);
    }

    return false;
  }

  /**
   * Start a separate thread for polling the JVM for heap memory usage.
   */
  private void startMemoryPoolPoller() {
    if (tenuredMemoryPoolMXBean == null) {
      return;
    }

    pollerExecutor = LoggingExecutors.newScheduledThreadPool(1, "GemfireHeapPoller");
    pollerExecutor.scheduleAtFixedRate(new HeapPoller(), POLLER_INTERVAL, POLLER_INTERVAL,
        TimeUnit.MILLISECONDS);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Started GemfireHeapPoller to poll the heap every " + POLLER_INTERVAL + " milliseconds");
    }
  }

  void setCriticalThreshold(final float criticalThreshold) {
    synchronized (this) {
      // If the threshold isn't changing then don't do anything.
      if (criticalThreshold == thresholds.getCriticalThreshold()) {
        return;
      }

      // Do some basic sanity checking on the new threshold
      if (criticalThreshold > 100.0f || criticalThreshold < 0.0f) {
        throw new IllegalArgumentException(
            "Critical percentage must be greater than 0.0 and less than or equal to 100.0.");
      }
      if (getTenuredMemoryPoolMXBean() == null) {
        throw new IllegalStateException(
            String.format("No tenured pools found.  Known pools are: %s",
                getAllMemoryPoolNames()));
      }
      if (criticalThreshold != 0 && thresholds.isEvictionThresholdEnabled()
          && criticalThreshold <= thresholds.getEvictionThreshold()) {
        throw new IllegalArgumentException(
            "Critical percentage must be greater than the eviction percentage.");
      }

      cache.setQueryMonitorRequiredForResourceManager(criticalThreshold != 0);

      thresholds = new MemoryThresholds(thresholds.getMaxMemoryBytes(), criticalThreshold,
          thresholds.getEvictionThreshold());

      updateStateAndSendEvent();

      // Start or stop monitoring based upon whether a threshold has been set
      if (thresholds.isEvictionThresholdEnabled()
          || thresholds.isCriticalThresholdEnabled()) {
        startMonitoring();
      } else if (!thresholds.isEvictionThresholdEnabled()
          && !thresholds.isCriticalThresholdEnabled()) {
        stopMonitoring();
      }

      stats.changeCriticalThreshold(thresholds.getCriticalThresholdBytes());
    }
  }

  @Override
  public boolean hasEvictionThreshold() {
    return hasEvictionThreshold;
  }

  void setEvictionThreshold(final float evictionThreshold) {
    hasEvictionThreshold = true;

    synchronized (this) {
      // If the threshold isn't changing then don't do anything.
      if (evictionThreshold == thresholds.getEvictionThreshold()) {
        return;
      }

      // Do some basic sanity checking on the new threshold
      if (evictionThreshold > 100.0f || evictionThreshold < 0.0f) {
        throw new IllegalArgumentException(
            "Eviction percentage must be greater than 0.0 and less than or equal to 100.0.");
      }
      if (getTenuredMemoryPoolMXBean() == null) {
        throw new IllegalStateException(
            String.format("No tenured pools found.  Known pools are: %s",
                getAllMemoryPoolNames()));
      }
      if (evictionThreshold != 0 && thresholds.isCriticalThresholdEnabled()
          && evictionThreshold >= thresholds.getCriticalThreshold()) {
        throw new IllegalArgumentException(
            "Eviction percentage must be less than the critical percentage.");
      }

      thresholds = new MemoryThresholds(thresholds.getMaxMemoryBytes(),
          thresholds.getCriticalThreshold(), evictionThreshold);

      updateStateAndSendEvent();

      // Start or stop monitoring based upon whether a threshold has been set
      if (thresholds.isEvictionThresholdEnabled()
          || thresholds.isCriticalThresholdEnabled()) {
        startMonitoring();
      } else if (!thresholds.isEvictionThresholdEnabled()
          && !thresholds.isCriticalThresholdEnabled()) {
        stopMonitoring();
      }

      stats.changeEvictionThreshold(thresholds.getEvictionThresholdBytes());
    }
  }

  /**
   * Compare the number of bytes used (fetched from the JVM) to the thresholds. If necessary, change
   * the state and send an event for the state change.
   */
  public void updateStateAndSendEvent() {
    updateStateAndSendEvent(
        testBytesUsedForThresholdSet != -1 ? testBytesUsedForThresholdSet : getBytesUsed(),
        "notification");
  }

  /**
   * Compare the number of bytes used to the thresholds. If necessary, change the state and send an
   * event for the state change.
   *
   * @param bytesUsed Number of bytes of heap memory currently used.
   * @param eventOrigin Indicates where the event originated e.g. notification vs polling
   */
  public void updateStateAndSendEvent(long bytesUsed, String eventOrigin) {
    stats.changeTenuredHeapUsed(bytesUsed);
    synchronized (this) {
      MemoryState oldState = mostRecentEvent.getState();
      MemoryState newState = thresholds.computeNextState(oldState, bytesUsed);
      if (oldState != newState) {
        setUsageThresholdOnMXBean(bytesUsed);

        currentState = newState;

        MemoryEvent event = new MemoryEvent(ResourceType.HEAP_MEMORY, oldState, newState,
            cache.getMyId(), bytesUsed, true, thresholds);

        upcomingEvent.set(event);
        processLocalEvent(event, eventOrigin);
        updateStatsFromEvent(event);

        // The state didn't change. However, if the state isn't normal and the
        // number of bytes used changed, then go ahead and send the event
        // again with an updated number of bytes used.
      } else if (!oldState.isNormal() && bytesUsed != mostRecentEvent.getBytesUsed()) {
        MemoryEvent event = new MemoryEvent(ResourceType.HEAP_MEMORY, oldState, newState,
            cache.getMyId(), bytesUsed, true, thresholds);
        upcomingEvent.set(event);
        processLocalEvent(event, eventOrigin);
      }
    }
  }

  /**
   * Update resource manager stats based upon the given event.
   *
   * @param event Event from which to derive data for updating stats.
   */
  private void updateStatsFromEvent(MemoryEvent event) {
    if (event.isLocal()) {
      if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
        stats.incHeapCriticalEvents();
      } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
        stats.incHeapSafeEvents();
      }

      if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
        stats.incEvictionStartEvents();
      } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
        stats.incEvictionStopEvents();
      }
    }
  }

  /**
   * Populate heap memory data in the given profile.
   *
   * @param profile Profile to populate.
   */
  @Override
  public void fillInProfile(final ResourceManagerProfile profile) {
    final MemoryEvent tempEvent = upcomingEvent.get();
    if (tempEvent != null) {
      mostRecentEvent = tempEvent;
      upcomingEvent.set(null);
    }
    final MemoryEvent eventToPopulate = mostRecentEvent;
    profile.setHeapData(eventToPopulate.getBytesUsed(), eventToPopulate.getState(),
        eventToPopulate.getThresholds());
  }

  @Override
  public MemoryState getState() {
    return currentState;
  }

  @Override
  public MemoryThresholds getThresholds() {
    MemoryThresholds saveThresholds = thresholds;

    return new MemoryThresholds(saveThresholds.getMaxMemoryBytes(),
        saveThresholds.getCriticalThreshold(), saveThresholds.getEvictionThreshold());
  }

  /**
   * Sets the usage threshold on the tenured pool to either the eviction threshold or the critical
   * threshold depending on the current number of bytes used
   *
   * @param bytesUsed Number of bytes of heap memory currently used.
   */
  private void setUsageThresholdOnMXBean(final long bytesUsed) {
    //// this method has been made a no-op to fix bug 49064
  }

  /**
   * Register with the JVM to get threshold events.
   *
   * Package private for testing.
   */
  void startJVMThresholdListener() {
    final MemoryPoolMXBean memoryPoolMXBean = getTenuredMemoryPoolMXBean();

    // Set collection threshold to a low value, so that we can get
    // notifications after every GC run. After each such collection
    // threshold notification we set the usage thresholds to an
    // appropriate value.
    if (!testDisableMemoryUpdates) {
      memoryPoolMXBean.setCollectionUsageThreshold(1);
    }

    final long usageThreshold = memoryPoolMXBean.getUsageThreshold();
    cache.getLogger().info(
        String.format("Overridding MemoryPoolMXBean heap threshold bytes %s on pool %s",
            usageThreshold, memoryPoolMXBean.getName()));

    MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    NotificationEmitter emitter = (NotificationEmitter) mbean;
    emitter.addNotificationListener(this, null, null);
  }

  /**
   * Returns the number of bytes of memory reported by the tenured pool as currently in use.
   */
  @Override
  public long getBytesUsed() {
    return getTenuredMemoryPoolMXBean().getUsage().getUsed();
  }

  public static long getTenuredPoolMaxMemory() {
    return tenuredPoolMaxMemory;
  }

  /**
   * Deliver a memory event from one of the monitors to both local listeners and remote resource
   * managers. Also, if a critical event is received and a query monitor has been enabled, then the
   * query monitor will be notified.
   *
   * Package private for testing.
   *
   * @param event Event to process.
   * @param eventOrigin Indicates where the event originated e.g. notification vs polling
   */
  synchronized void processLocalEvent(MemoryEvent event, String eventOrigin) {
    assert event.isLocal();

    if (logger.isDebugEnabled()) {
      logger.debug("Handling new local event " + event);
    }

    if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
      cache.getLogger().error(
          createCriticalThresholdLogMessage(event, eventOrigin, true));
      if (!cache.isQueryMonitorDisabledForLowMemory()) {
        cache.getQueryMonitor().setLowMemory(true, event.getBytesUsed());
      }

    } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
      cache.getLogger().error(
          createCriticalThresholdLogMessage(event, eventOrigin, false));
      if (!cache.isQueryMonitorDisabledForLowMemory()) {
        cache.getQueryMonitor().setLowMemory(false, event.getBytesUsed());
      }
    }

    if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
      cache.getLogger().info(String.format("Member: %s above %s eviction threshold",
          event.getMember(), "heap"));
    } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
      cache.getLogger().info(String.format("Member: %s below %s eviction threshold",
          event.getMember(), "heap"));
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Informing remote members of event " + event);
    }

    resourceAdvisor.updateRemoteProfile();
    resourceManager.deliverLocalEvent(event);
  }

  @Override
  public void notifyListeners(final Set<ResourceListener<?>> listeners,
      final ResourceEvent event) {
    for (ResourceListener listener : listeners) {
      try {
        listener.onEvent(event);
      } catch (CancelException ignore) {
        // ignore
      } catch (Throwable t) {
        Error err;
        if (t instanceof Error && SystemFailure.isJVMFailureError(err = (Error) t)) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned now, so
          // don't let this thread continue.
          throw err;
        }
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        cache.getLogger()
            .error("Exception occurred when notifying listeners ", t);
      }
    }
  }

  // Handles memory usage notification from MemoryMXBean.
  // See ((NotificationEmitter) MemoryMXBean).addNoticiationListener(...).
  @Override
  public void handleNotification(final Notification notification, final Object callback) {
    resourceManager.runWithNotifyExecutor(() -> {
      // Not using the information given by the notification in favor
      // of constructing fresh information ourselves.
      if (!testDisableMemoryUpdates) {
        tenuredHeapConsumptionMonitor.checkTenuredHeapConsumption(notification);
        updateStateAndSendEvent();
      }
    });
  }

  public Set<DistributedMember> getHeapCriticalMembersFrom(
      Set<? extends DistributedMember> members) {
    Set<DistributedMember> criticalMembers = getCriticalMembers();
    criticalMembers.retainAll(members);
    return criticalMembers;
  }

  private Set<DistributedMember> getCriticalMembers() {
    Set<DistributedMember> criticalMembers = new HashSet<>(resourceAdvisor.adviseCriticalMembers());
    if (mostRecentEvent.getState().isCritical()) {
      criticalMembers.add(cache.getMyId());
    }
    return criticalMembers;
  }

  public void checkForLowMemory(Function function, DistributedMember targetMember) {
    Set<DistributedMember> targetMembers = Collections.singleton(targetMember);
    checkForLowMemory(function, targetMembers);
  }

  public void checkForLowMemory(Function function, Set<? extends DistributedMember> dest) {
    LowMemoryException exception = createLowMemoryIfNeeded(function, dest);
    if (exception != null) {
      throw exception;
    }
  }

  public LowMemoryException createLowMemoryIfNeeded(Function function,
      DistributedMember targetMember) {
    Set<DistributedMember> targetMembers = Collections.singleton(targetMember);
    return createLowMemoryIfNeeded(function, targetMembers);
  }

  public LowMemoryException createLowMemoryIfNeeded(Function function,
      Set<? extends DistributedMember> memberSet) {
    if (function.optimizeForWrite() && !(function instanceof AllowExecutionInLowMemory)
        && !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      Set<DistributedMember> criticalMembersFrom = getHeapCriticalMembersFrom(memberSet);
      if (!criticalMembersFrom.isEmpty()) {
        return new LowMemoryException(
            String.format(
                "Function: %s cannot be executed because the members %s are running low on memory",
                function.getId(), criticalMembersFrom),
            criticalMembersFrom);
      }
    }
    return null;
  }


  /**
   * Determines if the given member is in a heap critical state.
   *
   * @param member Member to check.
   *
   * @return True if the member's heap memory is in a critical state, false otherwise.
   */
  public boolean isMemberHeapCritical(final InternalDistributedMember member) {
    if (member.equals(cache.getMyId())) {
      return mostRecentEvent.getState().isCritical();
    }
    return resourceAdvisor.isHeapCritical(member);
  }

  protected MemoryEvent getMostRecentEvent() {
    return mostRecentEvent;
  }

  @VisibleForTesting
  protected HeapMemoryMonitor setMostRecentEvent(
      MemoryEvent mostRecentEvent) {
    this.mostRecentEvent = mostRecentEvent;
    return this;
  }

  class LocalHeapStatListener implements LocalStatListener {
    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.statistics.LocalStatListener#statValueChanged(double)
     */
    @Override
    @SuppressWarnings("synthetic-access")
    public void statValueChanged(double value) {
      final long usedBytes = (long) value;
      try {
        resourceManager.runWithNotifyExecutor(() -> {
          if (!testDisableMemoryUpdates) {
            updateStateAndSendEvent(usedBytes, "polling");
          }
        });
        if (HeapMemoryMonitor.logger.isDebugEnabled()) {
          HeapMemoryMonitor.logger.debug(
              "StatSampler scheduled a " + "handleNotification call with " + usedBytes + " bytes");
        }
      } catch (RejectedExecutionException ignore) {
        if (!resourceManager.isClosed()) {
          logger.warn("No memory events will be delivered because of RejectedExecutionException");
        }
      } catch (CacheClosedException ignore) {
        // nothing to do
      }
    }
  }

  @Override
  public String toString() {
    return "HeapMemoryMonitor [thresholds=" + thresholds + ", mostRecentEvent="
        + mostRecentEvent + "]";
  }

  /**
   * Polls the heap if stat sampling is disabled.
   */
  class HeapPoller implements Runnable {
    @SuppressWarnings("synthetic-access")
    @Override
    public void run() {
      if (testDisableMemoryUpdates) {
        return;
      }
      try {
        updateStateAndSendEvent(getBytesUsed(), "polling");
      } catch (Exception e) {
        HeapMemoryMonitor.logger.debug("Poller Thread caught exception:", e);
      }
      // TODO: do we need to handle errors too?
    }
  }

  /**
   * Overrides the value returned by the JVM as the number of bytes of available memory.
   *
   * @param testMaxMemoryBytes The value to use as the maximum number of bytes of memory available.
   */
  public void setTestMaxMemoryBytes(final long testMaxMemoryBytes) {
    synchronized (this) {
      MemoryThresholds newThresholds;

      if (testMaxMemoryBytes == 0) {
        newThresholds = new MemoryThresholds(getTenuredPoolMaxMemory());
      } else {
        newThresholds = new MemoryThresholds(testMaxMemoryBytes,
            thresholds.getCriticalThreshold(), thresholds.getEvictionThreshold());
      }

      thresholds = newThresholds;
      final String builder =
          "In testing, the following values were set" + " maxMemoryBytes:"
              + newThresholds.getMaxMemoryBytes()
              + " criticalThresholdBytes:" + newThresholds.getCriticalThresholdBytes()
              + " evictionThresholdBytes:" + newThresholds.getEvictionThresholdBytes();
      logger.debug(builder);
    }
  }

  public static void setTestDisableMemoryUpdates(final boolean newTestDisableMemoryUpdates) {
    testDisableMemoryUpdates = newTestDisableMemoryUpdates;
  }

  /**
   * Since the setter methods for the eviction and critical thresholds immediately update state
   * based upon the new threshold value and the number of bytes currently used by the JVM, there
   * needs to be a way to override the number of bytes of memory reported as in use for testing.
   * That's what this method and the value it sets are for.
   *
   * @param newTestBytesUsedForThresholdSet Value to use as the amount of memory in use when calling
   *        the setEvictionThreshold or setCriticalThreshold methods are called.
   */
  public static void setTestBytesUsedForThresholdSet(final long newTestBytesUsedForThresholdSet) {
    testBytesUsedForThresholdSet = newTestBytesUsedForThresholdSet;
  }

  private String createCriticalThresholdLogMessage(MemoryEvent event, String eventOrigin,
      boolean above) {
    return "Member: " + event.getMember() + " " + (above ? "above" : "below")
        + " heap critical threshold."
        + " Event generated via " + eventOrigin + "."
        + " Used bytes: " + event.getBytesUsed() + "."
        + " Memory thresholds: " + thresholds;
  }
}
