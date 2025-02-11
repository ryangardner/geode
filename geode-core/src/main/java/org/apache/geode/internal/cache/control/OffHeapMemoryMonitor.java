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

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState;
import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.MemoryUsageListener;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Allows for the setting of eviction and critical thresholds. These thresholds are compared against
 * current off-heap usage and, with the help of {#link InternalResourceManager}, dispatches events
 * when the thresholds are crossed.
 *
 * @since Geode 1.0
 */
public class OffHeapMemoryMonitor implements MemoryMonitor, MemoryUsageListener {
  private static final Logger logger = LogService.getLogger();
  private volatile MemoryThresholds thresholds = new MemoryThresholds(0);
  private volatile MemoryEvent mostRecentEvent = new MemoryEvent(ResourceType.OFFHEAP_MEMORY,
      MemoryState.DISABLED, MemoryState.DISABLED, null, 0L, true, thresholds);
  private volatile MemoryState currentState = MemoryState.DISABLED;

  // Set when startMonitoring() and stopMonitoring() are called
  // Package private for testing
  Boolean started = false;

  // Set to true when setEvictionThreshold(...) is called.
  private boolean hasEvictionThreshold = false;

  private Thread memoryListenerThread;

  private final OffHeapMemoryUsageListener offHeapMemoryUsageListener;
  private final InternalResourceManager resourceManager;
  private final ResourceAdvisor resourceAdvisor;
  private final InternalCache cache;
  private final ResourceManagerStats stats;
  /**
   * InternalResoruceManager insists on creating a OffHeapMemoryMonitor even when it does not have
   * off-heap memory. So we need to handle memoryAllocator being null.
   */
  private final MemoryAllocator memoryAllocator;

  OffHeapMemoryMonitor(final InternalResourceManager resourceManager, final InternalCache cache,
      final MemoryAllocator memoryAllocator, final ResourceManagerStats stats) {
    this.resourceManager = resourceManager;
    resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    this.cache = cache;
    this.stats = stats;

    this.memoryAllocator = memoryAllocator;
    if (memoryAllocator != null) {
      thresholds = new MemoryThresholds(this.memoryAllocator.getTotalMemory());
    }

    offHeapMemoryUsageListener = new OffHeapMemoryUsageListener();
  }

  /**
   * Start monitoring off-heap memory usage by adding this as a listener to the off-heap memory
   * allocator.
   */
  private void startMonitoring() {
    synchronized (this) {
      if (started) {
        return;
      }

      Thread t =
          new LoggingThread("OffHeapMemoryListener", offHeapMemoryUsageListener);
      t.setPriority(Thread.MAX_PRIORITY);
      t.start();
      memoryListenerThread = t;

      memoryAllocator.addMemoryUsageListener(this);

      started = true;
    }
  }

  /**
   * Stop monitoring off-heap usage.
   */
  @Override
  public void stopMonitoring() {
    stopMonitoring(false);
  }

  public void stopMonitoring(boolean waitForThread) {
    Thread threadToWaitFor = null;
    synchronized (this) {
      if (!started) {
        return;
      }

      memoryAllocator.removeMemoryUsageListener(this);

      offHeapMemoryUsageListener.stop();
      if (waitForThread) {
        threadToWaitFor = memoryListenerThread;
      }
      memoryListenerThread = null;
      started = false;
    }
    if (threadToWaitFor != null) {
      try {
        threadToWaitFor.join();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public volatile OffHeapMemoryMonitorObserver testHook;

  /**
   * Used by unit tests to be notified when OffHeapMemoryMonitor does something.
   */
  public interface OffHeapMemoryMonitorObserver {
    /**
     * Called at the beginning of updateMemoryUsed.
     *
     * @param bytesUsed the number of bytes of off-heap memory currently used
     * @param willSendEvent true if an event will be sent to the OffHeapMemoryUsageListener.
     */
    void beginUpdateMemoryUsed(long bytesUsed, boolean willSendEvent);

    void afterNotifyUpdateMemoryUsed(long bytesUsed);

    /**
     * Called at the beginning of updateStateAndSendEvent.
     *
     * @param bytesUsed the number of bytes of off-heap memory currently used
     * @param willSendEvent true if an event will be sent to the OffHeapMemoryUsageListener.
     */
    void beginUpdateStateAndSendEvent(long bytesUsed, boolean willSendEvent);

    void updateStateAndSendEventBeforeProcess(long bytesUsed, MemoryEvent event);

    void updateStateAndSendEventBeforeAbnormalProcess(long bytesUsed, MemoryEvent event);

    void updateStateAndSendEventIgnore(long bytesUsed, MemoryState oldState, MemoryState newState,
        long mostRecentBytesUsed, boolean deliverNextAbnormalEvent);
  }

  @Override
  public void updateMemoryUsed(final long bytesUsed) {
    final boolean willSendEvent = mightSendEvent(bytesUsed);
    final OffHeapMemoryMonitorObserver _testHook = testHook;
    if (_testHook != null) {
      _testHook.beginUpdateMemoryUsed(bytesUsed, willSendEvent);
    }
    if (!willSendEvent) {
      return;
    }
    offHeapMemoryUsageListener.deliverEvent();
    if (_testHook != null) {
      _testHook.afterNotifyUpdateMemoryUsed(bytesUsed);
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
      if (memoryAllocator == null) {
        throw new IllegalStateException(
            "No off-heap memory has been configured.");
      }
      if (criticalThreshold != 0 && thresholds.isEvictionThresholdEnabled()
          && criticalThreshold <= thresholds.getEvictionThreshold()) {
        throw new IllegalArgumentException(
            "Critical percentage must be greater than the eviction percentage.");
      }

      cache.setQueryMonitorRequiredForResourceManager(criticalThreshold != 0);

      thresholds = new MemoryThresholds(thresholds.getMaxMemoryBytes(), criticalThreshold,
          thresholds.getEvictionThreshold());

      updateStateAndSendEvent(getBytesUsed());

      // Start or stop monitoring based upon whether a threshold has been set
      if (thresholds.isEvictionThresholdEnabled()
          || thresholds.isCriticalThresholdEnabled()) {
        startMonitoring();
      } else if (!thresholds.isEvictionThresholdEnabled()
          && !thresholds.isCriticalThresholdEnabled()) {
        stopMonitoring();
      }

      stats.changeOffHeapCriticalThreshold(thresholds.getCriticalThresholdBytes());
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
      if (memoryAllocator == null) {
        throw new IllegalStateException(
            "No off-heap memory has been configured.");
      }
      if (evictionThreshold != 0 && thresholds.isCriticalThresholdEnabled()
          && evictionThreshold >= thresholds.getCriticalThreshold()) {
        throw new IllegalArgumentException(
            "Eviction percentage must be less than the critical percentage.");
      }

      thresholds = new MemoryThresholds(thresholds.getMaxMemoryBytes(),
          thresholds.getCriticalThreshold(), evictionThreshold);

      updateStateAndSendEvent(getBytesUsed());

      // Start or stop monitoring based upon whether a threshold has been set
      if (thresholds.isEvictionThresholdEnabled()
          || thresholds.isCriticalThresholdEnabled()) {
        startMonitoring();
      } else if (!thresholds.isEvictionThresholdEnabled()
          && !thresholds.isCriticalThresholdEnabled()) {
        stopMonitoring();
      }

      stats.changeOffHeapEvictionThreshold(thresholds.getEvictionThresholdBytes());
    }
  }

  /**
   * Compare the number of bytes used (fetched from the JVM) to the thresholds. If necessary, change
   * the state and send an event for the state change.
   *
   * @return true if an event was sent
   */
  public boolean updateStateAndSendEvent() {
    return updateStateAndSendEvent(getBytesUsed());
  }

  /**
   * Compare the number of bytes used to the thresholds. If necessary, change the state and send an
   * event for the state change.
   *
   * Public for testing.
   *
   * @param bytesUsed Number of bytes of off-heap memory currently used.
   * @return true if an event was sent
   */
  public boolean updateStateAndSendEvent(long bytesUsed) {
    boolean result = false;
    synchronized (this) {
      final MemoryEvent mre = mostRecentEvent;
      final MemoryState oldState = mre.getState();
      final MemoryThresholds thresholds = this.thresholds;
      final OffHeapMemoryMonitorObserver _testHook = testHook;
      MemoryState newState = thresholds.computeNextState(oldState, bytesUsed);
      if (oldState != newState) {
        currentState = newState;

        MemoryEvent event = new MemoryEvent(ResourceType.OFFHEAP_MEMORY, oldState, newState,
            cache.getMyId(), bytesUsed, true, thresholds);
        if (_testHook != null) {
          _testHook.updateStateAndSendEventBeforeProcess(bytesUsed, event);
        }
        mostRecentEvent = event;
        processLocalEvent(event);
        updateStatsFromEvent(event);
        result = true;

      } else if (!oldState.isNormal() && bytesUsed != mre.getBytesUsed()
          && deliverNextAbnormalEvent) {
        deliverNextAbnormalEvent = false;
        MemoryEvent event = new MemoryEvent(ResourceType.OFFHEAP_MEMORY, oldState, newState,
            cache.getMyId(), bytesUsed, true, thresholds);
        if (_testHook != null) {
          _testHook.updateStateAndSendEventBeforeAbnormalProcess(bytesUsed, event);
        }
        mostRecentEvent = event;
        processLocalEvent(event);
        result = true;
      } else {
        if (_testHook != null) {
          _testHook.updateStateAndSendEventIgnore(bytesUsed, oldState, newState, mre.getBytesUsed(),
              deliverNextAbnormalEvent);
        }

      }
    }
    return result;
  }

  /**
   * Return true if the given number of bytes compared to the current monitor state would generate a
   * new memory event.
   *
   * @param bytesUsed Number of bytes of off-heap memory currently used.
   * @return true if a new event might need to be sent
   */
  private boolean mightSendEvent(long bytesUsed) {
    final MemoryEvent mre = mostRecentEvent;
    final MemoryState oldState = mre.getState();
    final MemoryThresholds thresholds = mre.getThresholds();
    MemoryState newState = thresholds.computeNextState(oldState, bytesUsed);
    if (oldState != newState) {
      return true;
    } else
      return !oldState.isNormal() && bytesUsed != mre.getBytesUsed()
          && deliverNextAbnormalEvent;
  }

  private volatile boolean deliverNextAbnormalEvent = false;

  /**
   * Used by the OffHeapMemoryUsageListener to tell us that the next abnormal event should be
   * delivered even if the state does not change as long as the memory usage changed. For some
   * reason, unknown to me, if we stay in an abnormal state for more than a second then we want to
   * send another event to update the memory usage.
   */
  void deliverNextAbnormalEvent() {
    deliverNextAbnormalEvent = true;
  }

  /**
   * Update resource manager stats based upon the given event.
   *
   * @param event Event from which to derive data for updating stats.
   */
  private void updateStatsFromEvent(MemoryEvent event) {
    if (event.isLocal()) {
      if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
        stats.incOffHeapCriticalEvents();
      } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
        stats.incOffHeapSafeEvents();
      }

      if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
        stats.incOffHeapEvictionStartEvents();
      } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
        stats.incOffHeapEvictionStopEvents();
      }
    }
  }

  /**
   * Populate off-heap memory data in the given profile.
   *
   * @param profile Profile to populate.
   */
  @Override
  public void fillInProfile(final ResourceManagerProfile profile) {
    final MemoryEvent eventToPopulate = mostRecentEvent;
    profile.setOffHeapData(eventToPopulate.getBytesUsed(), eventToPopulate.getState(),
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
   * Returns the number of bytes of memory reported by the memory allocator as currently in use.
   */
  @Override
  public long getBytesUsed() {
    if (memoryAllocator == null) {
      return 0;
    }
    return memoryAllocator.getUsedMemory();
  }

  /**
   * Deliver a memory event from one of the monitors to both local listeners and remote resource
   * managers. Also, if a critical event is received and a query monitor has been enabled, then the
   * query monitor will be notified.
   *
   * Package private for testing.
   *
   * @param event Event to process.
   */
  void processLocalEvent(MemoryEvent event) {
    assert event.isLocal();

    if (logger.isDebugEnabled()) {
      logger.debug("Handling new local event {}", event);
    }

    if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
      logger.error("Member: {} above {} critical threshold",
          new Object[] {event.getMember(), "off-heap"});
    } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
      logger.error("Member: {} below {} critical threshold",
          new Object[] {event.getMember(), "off-heap"});
    }

    if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
      logger
          .info("Member: {} above {} eviction threshold",
              new Object[] {event.getMember(), "off-heap"});
    } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
      logger
          .info("Member: {} below {} eviction threshold",
              new Object[] {event.getMember(), "off-heap"});
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Informing remote members of event {}", event);
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
        logger.error("Exception occurred when notifying listeners ", t);
      }
    }
  }

  @Override
  public String toString() {
    return "OffHeapMemoryMonitor [thresholds=" + thresholds + ", mostRecentEvent="
        + mostRecentEvent + "]";
  }

  class OffHeapMemoryUsageListener implements Runnable {
    private boolean deliverEvent = false;
    private boolean stopRequested = false;

    OffHeapMemoryUsageListener() {}

    public synchronized void deliverEvent() {
      deliverEvent = true;
      notifyAll();
    }

    public synchronized void stop() {
      stopRequested = true;
      notifyAll();
    }

    @Override
    public void run() {
      if (logger.isDebugEnabled()) {
        logger.debug("OffHeapMemoryUsageListener is starting {}", this);
      }
      int callsWithNoEvent = 0;
      final int MS_TIMEOUT = 10;
      final int MAX_CALLS_WITH_NO_EVENT = 1000 / MS_TIMEOUT;
      boolean exitRunLoop = false;
      while (!exitRunLoop) {
        if (!updateStateAndSendEvent()) {
          callsWithNoEvent++;
          if (callsWithNoEvent > MAX_CALLS_WITH_NO_EVENT) {
            deliverNextAbnormalEvent();
            callsWithNoEvent = 0;
          }
        } else {
          callsWithNoEvent = 0;
        }

        synchronized (this) {
          if (stopRequested) {
            exitRunLoop = true;
          } else if (deliverEvent) {
            // No need to wait.
            // Loop around and call updateStateAndSendEvent.
            deliverEvent = false;
          } else {
            // Wait to be notified that off-heap memory changed
            // or for the wait to timeout.
            // In some cases we need to generate an event even
            // when we have not been notified (see GEODE-438).
            // So we don't want this wait to be for very long.
            try {
              wait(MS_TIMEOUT);
              deliverEvent = false;
            } catch (InterruptedException ignore) {
              logger.warn("OffHeapMemoryUsageListener was interrupted {}", this);
              stopRequested = true;
              exitRunLoop = true;
            }
          }
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("OffHeapMemoryUsageListener is stopping {}", this);
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " Thread" + " #" + System.identityHashCode(this);
    }
  }
}
