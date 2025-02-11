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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class CommonParallelGatewaySenderDUnitTest extends WANTestBase {

  @Test
  public void testSameSenderWithNonColocatedRegions() throws Exception {
    IgnoredException.addIgnoredException("cannot have the same parallel");
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    try {
      vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1,
          100, isOffHeap()));
      fail("Expected IllegalStateException : cannot have the same parallel gateway sender");
    } catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException) || !(e.getCause().getMessage()
          .contains("cannot have the same parallel gateway sender id"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }

  /**
   * Simple scenario. Two regions attach the same PGS
   *
   * @throws Exception Below test is disabled intentionally 1> In this release 8.0, for rolling
   *         upgrade support queue name is changed to old style 2>Common parallel sender for
   *         different non colocated regions is not supported in 8.0 so no need to bother about
   *         ParallelGatewaySenderQueue#convertPathToName 3> We have to enabled it in next release
   *         4> Version based rolling upgrade support should be provided. based on the version of
   *         the gemfire QSTRING should be used between 8.0 and version prior to 8.0
   */
  @Test
  @Ignore("TODO")
  public void testParallelPropagation() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", null, 1, 100,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", null, 1, 100,
        isOffHeap()));
    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR1", 1000));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR2", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR1", 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR2", 1000));
  }

  /**
   * The PGS is persistence enabled but not the Regions Below test is disabled intentionally 1> In
   * this release 8.0, for rolling upgrade support queue name is changed to old style 2>Common
   * parallel sender for different non colocated regions is not supported in 8.0 so no need to
   * bother about ParallelGatewaySenderQueue#convertPathToName 3> We have to enabled it in next
   * release 4> Version based rolling upgrade support should be provided. based on the version of
   * the gemfire QSTRING should be used between 8.0 and version prior to 8.0
   */
  @Test
  @Ignore("TODO")
  public void testParallelPropagationPersistenceEnabled() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR1", null, 1, 100,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR2", null, 1, 100,
        isOffHeap()));
    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR1", 1000));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR2", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> CommonParallelGatewaySenderDUnitTest
        .validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR1", 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR2", 1000));
  }


  /**
   * Enable persistence for GatewaySender. Pause the sender and do some puts in local region. Close
   * the local site and rebuild the region and sender from disk store. Dispatcher should not start
   * dispatching events recovered from persistent sender. Check if the remote site receives all the
   * events. Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade
   * support queue name is changed to old style 2>Common parallel sender for different non colocated
   * regions is not supported in 8.0 so no need to bother about
   * ParallelGatewaySenderQueue#convertPathToName 3> We have to enabled it in next release 4>
   * Version based rolling upgrade support should be provided. based on the version of the gemfire
   * QSTRING should be used between 8.0 and version prior to 8.0
   */
  @Test
  @Ignore("TODO")
  public void testPRWithGatewaySenderPersistenceEnabled_Restart() {
    // create locator on local site
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    // create locator on remote site
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    // create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter()
        .info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);

    // create PR on remote site
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR1", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR1", null, 1, 100,
        isOffHeap()));

    // create PR on remote site
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2", null, 1, 100,
        isOffHeap()));

    // create PR on local site
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100,
        isOffHeap()));

    // create PR on local site
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2", "ln", 1, 100,
        isOffHeap()));


    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // wait for senders to become running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // pause the senders
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "PR1", 3000));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "PR2", 5000));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // --------------------close and rebuild local site
    // -------------------------------------------------
    // kill the senders
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());

    LogWriterUtils.getLogWriter().info("Killed all the senders.");

    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    LogWriterUtils.getLogWriter().info("Created back the cache");

    // create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore1, true));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, true));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore3, true));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore4, true));

    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    // create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase
        .createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100, isOffHeap()));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase
        .createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100, isOffHeap()));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase
        .createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100, isOffHeap()));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase
        .createPartitionedRegion(getTestMethodName() + "PR1", "ln", 1, 100, isOffHeap()));

    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    inv1 = vm4.invokeAsync(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2",
        "ln", 1, 100, isOffHeap()));
    inv2 = vm5.invokeAsync(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2",
        "ln", 1, 100, isOffHeap()));
    inv3 = vm6.invokeAsync(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2",
        "ln", 1, 100, isOffHeap()));
    inv4 = vm7.invokeAsync(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PR2",
        "ln", 1, 100, isOffHeap()));

    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    LogWriterUtils.getLogWriter().info("Created back the partitioned regions");

    // start the senders in async mode. This will ensure that the
    // node of shadow PR that went down last will come up first
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    LogWriterUtils.getLogWriter().info("All the senders are now running...");

    // ----------------------------------------------------------------------------------------------------

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "PR1", 3000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "PR1", 3000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "PR2", 5000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "PR2", 5000));
  }

  public static void validateParallelSenderQueueAllBucketsDrained(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    ConcurrentParallelGatewaySenderQueue regionQueue =
        (ConcurrentParallelGatewaySenderQueue) ((AbstractGatewaySender) sender).getQueues()
            .toArray(new RegionQueue[1])[0];

    Set<PartitionedRegion> shadowPRs = regionQueue.getRegions();

    for (PartitionedRegion shadowPR : shadowPRs) {
      Set<BucketRegion> buckets = shadowPR.getDataStore().getAllLocalBucketRegions();

      for (final BucketRegion bucket : buckets) {
        WaitCriterion wc = new WaitCriterion() {
          @Override
          public boolean done() {
            if (bucket.keySet().size() == 0) {
              getLogWriter().info("Bucket " + bucket.getId() + " is empty");
              return true;
            }
            return false;
          }

          @Override
          public String description() {
            return "Expected bucket entries for bucket: " + bucket.getId()
                + " is: 0 but actual entries: " + bucket.keySet().size()
                + " This bucket isPrimary: " + bucket.getBucketAdvisor().isPrimary() + " KEYSET: "
                + bucket.keySet();
          }
        };
        GeodeAwaitility.await().untilAsserted(wc);

      } // for loop ends
    }
  }

  @Test
  public void whenBatchBasedOnTimeOnlyThenQueueShouldNotDispatchUntilIntervalIsHit() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    int batchIntervalTime = 5000;

    // Create receiver and region
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 10,
            isOffHeap()));
    vm2.invoke(WANTestBase::createReceiver);
    vm2.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_PR"));

    // Create sender with batchSize disabled
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    String senderId = "ln";
    final String builder = String.valueOf(senderId);
    vm4.invoke(() -> {
      InternalGatewaySenderFactory gateway =
          (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(100);
      gateway.setBatchSize(RegionQueue.BATCH_BASED_ON_TIME_ONLY);
      gateway.setBatchConflationEnabled(true);
      gateway.setDispatcherThreads(5);
      gateway.setBatchTimeInterval(batchIntervalTime);
      gateway.create(senderId, 2);
    });

    // Create region with the sender ids
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        builder, 0, 10, isOffHeap()));

    // Do puts
    int numPuts = 100;
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", numPuts));

    // attempt to prove the absence of a dispatch/ prove a dispatch has not occurred
    // will verify that no events have occurred over a period of time less than batch interval but
    // more than enough
    // for a regular dispatch to have occurred
    vm2.invoke(() -> {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < batchIntervalTime - 1000) {
        assertEquals(0, listener1.getNumEvents());
      }
    });

    // Verify receiver listener events
    vm2.invoke(() -> WANTestBase.verifyListenerEvents(numPuts));
  }

}
