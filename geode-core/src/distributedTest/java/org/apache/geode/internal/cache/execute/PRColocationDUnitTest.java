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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Customer;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * This is the test for the custom and colocated partitioning of PartitionedRegion
 *
 */
@SuppressWarnings("synthetic-access")

public class PRColocationDUnitTest extends JUnit4CacheTestCase {

  VM accessor = null;

  VM dataStore1 = null;

  VM dataStore2 = null;

  VM dataStore3 = null;

  protected static Cache cache = null;

  protected static int totalNumBucketsInTest = 0;

  static final String CustomerPartitionedRegionName = "CustomerPartitionedRegion";

  static final String OrderPartitionedRegionName = "OrderPartitionedRegion";

  static final String ShipmentPartitionedRegionName = "ShipmentPartitionedRegion";

  String regionName = null;

  Integer redundancy = null;

  Integer localMaxmemory = null;

  Integer totalNumBuckets = null;

  String colocatedWith = null;

  Boolean isPartitionResolver = null;

  Object[] attributeObjects = null;

  public PRColocationDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS(); // isolate this test from others to avoid periodic CacheExistsExceptions
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    accessor = host.getVM(3);
  }

  /*
   * Test for bug 41820
   */
  @Test
  public void testDestroyColocatedPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "C";
    colocatedWith = SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    attributeObjects = new Object[] {SEPARATOR + "A"};
    dataStore1.invoke(PRColocationDUnitTest.class, "destroyPR", attributeObjects);
  }

  /*
   * Test for checking the colocation of the regions which forms the tree
   */
  @Test
  public void testColocatedPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "C";
    colocatedWith = SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = SEPARATOR + "B";
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "E";
    colocatedWith = SEPARATOR + "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "F";
    colocatedWith = SEPARATOR + "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "G";
    colocatedWith = SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "H";
    colocatedWith = SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "I";
    colocatedWith = SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "J";
    colocatedWith = SEPARATOR + "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "K";
    colocatedWith = SEPARATOR + "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "L";
    colocatedWith = SEPARATOR + "E";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "M";
    colocatedWith = SEPARATOR + "F";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "N";
    colocatedWith = SEPARATOR + "G";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "O";
    colocatedWith = SEPARATOR + "I";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("A"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("D"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("H"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("B"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("K"));
  }

  /*
   * Test for checking the colocation of the regions which forms the tree
   */
  @Test
  public void testColocatedPartitionedRegion_NoFullPath() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "C";
    colocatedWith = "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = "B";
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "E";
    colocatedWith = "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "F";
    colocatedWith = "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "G";
    colocatedWith = "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "H";
    colocatedWith = "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "I";
    colocatedWith = "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "J";
    colocatedWith = "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "K";
    colocatedWith = "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "L";
    colocatedWith = "E";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "M";
    colocatedWith = "F";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "N";
    colocatedWith = "G";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    regionName = "O";
    colocatedWith = "I";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("A"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("D"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("H"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("B"));

    accessor.invoke(() -> PRColocationDUnitTest.validateColocatedRegions("K"));
  }

  @Test
  public void testColocatedSubPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = 1;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = SEPARATOR + "rootA" + SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "C";
    colocatedWith = SEPARATOR + "rootA" + SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = SEPARATOR + "rootB" + SEPARATOR + "B";
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "E";
    colocatedWith = SEPARATOR + "rootB" + SEPARATOR + "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "F";
    colocatedWith = SEPARATOR + "rootB" + SEPARATOR + "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "G";
    colocatedWith = SEPARATOR + "rootC" + SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "H";
    colocatedWith = SEPARATOR + "rootC" + SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "I";
    colocatedWith = SEPARATOR + "rootC" + SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "J";
    colocatedWith = SEPARATOR + "rootD" + SEPARATOR + "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "K";
    colocatedWith = SEPARATOR + "rootD" + SEPARATOR + "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "L";
    colocatedWith = SEPARATOR + "rootE" + SEPARATOR + "E";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "M";
    colocatedWith = SEPARATOR + "rootF" + SEPARATOR + "F";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "N";
    colocatedWith = SEPARATOR + "rootG" + SEPARATOR + "G";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "O";
    colocatedWith = SEPARATOR + "rootI" + SEPARATOR + "I";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootA" + SEPARATOR + "A"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootD" + SEPARATOR + "D"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootH" + SEPARATOR + "H"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootB" + SEPARATOR + "B"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootK" + SEPARATOR + "K"));
  }

  @Test
  public void testColocatedSubPartitionedRegion_NoFullPath() throws Throwable {
    createCacheInAllVms();
    redundancy = 1;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "rootA" + SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "C";
    colocatedWith = "rootA" + SEPARATOR + "A";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = "rootB" + SEPARATOR + "B";
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "E";
    colocatedWith = "rootB" + SEPARATOR + "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "F";
    colocatedWith = "rootB" + SEPARATOR + "B";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "G";
    colocatedWith = "rootC" + SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "H";
    colocatedWith = "rootC" + SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "I";
    colocatedWith = "rootC" + SEPARATOR + "C";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "J";
    colocatedWith = "rootD" + SEPARATOR + "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "K";
    colocatedWith = "rootD" + SEPARATOR + "D";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "L";
    colocatedWith = "rootE" + SEPARATOR + "E";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "M";
    colocatedWith = "rootF" + SEPARATOR + "F";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "N";
    colocatedWith = "rootG" + SEPARATOR + "G";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    regionName = "O";
    colocatedWith = "rootI" + SEPARATOR + "I";
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createSubPartitionedRegion(attributeObjects);

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootA" + SEPARATOR + "A"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootD" + SEPARATOR + "D"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootH" + SEPARATOR + "H"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootB" + SEPARATOR + "B"));

    accessor
        .invoke(() -> PRColocationDUnitTest.validateColocatedRegions("rootK" + SEPARATOR + "K"));
  }

  @Test
  public void testColocatedPRWithAccessorOnDifferentNode1() throws Throwable {

    createCacheInAllVms();

    dataStore1.invoke(new CacheSerializableRunnable("testColocatedPRwithAccessorOnDifferentNode") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = Boolean.FALSE;
        redundancy = 0;
        localMaxmemory = 50;
        totalNumBuckets = 11;

        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxmemory)
            .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
        if (isPartitionResolver) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info("Partitioned Region " + partitionedRegionName
            + " created Successfully :" + pr);
      }
    });

    // add expected exception string
    final IgnoredException ex = IgnoredException.addIgnoredException(
        "Colocated regions should have accessors at the same node", dataStore1);
    dataStore1
        .invoke(new CacheSerializableRunnable("Colocated PR with Accessor on different nodes") {
          @Override
          public void run2() {
            regionName = OrderPartitionedRegionName;
            colocatedWith = CustomerPartitionedRegionName;
            isPartitionResolver = Boolean.FALSE;
            localMaxmemory = 0;
            redundancy = 0;
            totalNumBuckets = 11;
            PartitionAttributesFactory paf = new PartitionAttributesFactory();
            paf.setRedundantCopies(redundancy)
                .setLocalMaxMemory(localMaxmemory)
                .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
            if (isPartitionResolver) {
              paf.setPartitionResolver(
                  new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
            }
            PartitionAttributes prAttr = paf.create();
            AttributesFactory attr = new AttributesFactory();
            attr.setPartitionAttributes(prAttr);
            assertNotNull(basicGetCache());
            try {
              basicGetCache().createRegion(regionName, attr.create());
              fail("It should have failed with Exception: Colocated regions "
                  + "should have accessors at the same node");
            } catch (Exception Expected) {
              Expected.printStackTrace();
              LogWriterUtils.getLogWriter().info("Expected Message : " + Expected.getMessage());
              assertTrue(Expected.getMessage()
                  .startsWith("Colocated regions should have accessors at the same node"));
            }
          }
        });
    ex.remove();
  }

  @Test
  public void testColocatedPRWithAccessorOnDifferentNode2() throws Throwable {

    createCacheInAllVms();

    dataStore1.invoke(new CacheSerializableRunnable("testColocatedPRWithAccessorOnDifferentNode2") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = Boolean.FALSE;
        redundancy = 0;
        localMaxmemory = 0;
        totalNumBuckets = 11;
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxmemory)
            .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
        if (isPartitionResolver) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info("Partitioned Region " + partitionedRegionName
            + " created Successfully :" + pr);
      }
    });

    // add expected exception string
    final IgnoredException ex = IgnoredException.addIgnoredException(
        "Colocated regions should have accessors at the same node", dataStore1);
    dataStore1
        .invoke(new CacheSerializableRunnable("Colocated PR with accessor on different nodes") {
          @Override
          public void run2() {
            regionName = OrderPartitionedRegionName;
            colocatedWith = CustomerPartitionedRegionName;
            isPartitionResolver = Boolean.FALSE;
            redundancy = 0;
            localMaxmemory = 50;
            totalNumBuckets = 11;
            PartitionAttributesFactory paf = new PartitionAttributesFactory();
            paf.setRedundantCopies(redundancy)
                .setLocalMaxMemory(localMaxmemory)
                .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
            if (isPartitionResolver) {
              paf.setPartitionResolver(
                  new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
            }
            PartitionAttributes prAttr = paf.create();
            AttributesFactory attr = new AttributesFactory();
            attr.setPartitionAttributes(prAttr);
            assertNotNull(basicGetCache());
            try {
              basicGetCache().createRegion(regionName, attr.create());
              fail("It should have failed with Exception: Colocated regions "
                  + "should have accessors at the same node");
            } catch (Exception Expected) {
              LogWriterUtils.getLogWriter().info("Expected Message : " + Expected.getMessage());
              assertTrue(Expected.getMessage()
                  .startsWith("Colocated regions should have accessors at the same node"));
            }
          }
        });
    ex.remove();
  }

  @Test
  public void testColocatedPRWithPROnDifferentNode1() throws Throwable {

    createCacheInAllVms();

    dataStore1.invoke(new CacheSerializableRunnable("TestColocatedPRWithPROnDifferentNode") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = Boolean.FALSE;
        redundancy = 0;
        localMaxmemory = 20;
        totalNumBuckets = 11;
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxmemory)
            .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
        if (isPartitionResolver) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info("Partitioned Region " + partitionedRegionName
            + " created Successfully :" + pr);
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testColocatedPRwithPROnDifferentNode") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = Boolean.FALSE;
        redundancy = 0;
        localMaxmemory = 20;
        totalNumBuckets = 11;
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxmemory)
            .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
        if (isPartitionResolver) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info("Partitioned Region " + partitionedRegionName
            + " created Successfully :" + pr);
      }
    });

    // add expected exception string
    final IgnoredException ex =
        IgnoredException.addIgnoredException("Cannot create buckets", dataStore2);
    dataStore2.invoke(new CacheSerializableRunnable("Colocated PR with PR on different node") {
      @Override
      public void run2() {
        regionName = OrderPartitionedRegionName;
        colocatedWith = CustomerPartitionedRegionName;
        isPartitionResolver = Boolean.FALSE;
        redundancy = 0;
        localMaxmemory = 50;
        totalNumBuckets = 11;
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxmemory)
            .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
        if (isPartitionResolver) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        try {
          Region r = basicGetCache().createRegion(regionName, attr.create());
          // fail("It should have failed with Exception : Colocated regions
          // should have accessors at the same node");
          r.put("key", "value");
          fail("Failed because we did not receive the exception - : Cannot create buckets, "
              + "as colocated regions are not configured to be at the same nodes.");
        } catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Message : " + Expected.getMessage());
          assertTrue(Expected.getMessage().contains("Cannot create buckets, as "
              + "colocated regions are not configured to be at the same nodes."));
        }
      }
    });
    ex.remove();

    dataStore1.invoke(new CacheSerializableRunnable("Colocated PR with PR on different node") {
      @Override
      public void run2() {
        regionName = OrderPartitionedRegionName;
        colocatedWith = CustomerPartitionedRegionName;
        isPartitionResolver = Boolean.FALSE;
        redundancy = 0;
        localMaxmemory = 50;
        totalNumBuckets = 11;
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxmemory)
            .setTotalNumBuckets(totalNumBuckets).setColocatedWith(colocatedWith);
        if (isPartitionResolver) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        try {
          Region r = basicGetCache().createRegion(regionName, attr.create());
          r.put("key", "value");
          assertEquals("value", r.get("key"));
        } catch (Exception NotExpected) {
          NotExpected.printStackTrace();
          LogWriterUtils.getLogWriter()
              .info("Unexpected Exception Message : " + NotExpected.getMessage());
          Assert.fail("Unpexpected Exception", NotExpected);
        }
      }
    });
  }

  @Test
  public void testColocatedPRWithLocalDestroy() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;
    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.FALSE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.FALSE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Put the customer 1-10 in CustomerPartitionedRegion

    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor
        .invoke(() -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName));

    // add expected exception string
    final String expectedExMessage = "Any Region in colocation chain cannot be destroyed locally.";
    final IgnoredException ex = IgnoredException.addIgnoredException(expectedExMessage, dataStore1);
    dataStore1.invoke(new CacheSerializableRunnable("PR with Local destroy") {
      @Override
      public void run2() {
        Region partitionedregion =
            basicGetCache().getRegion(SEPARATOR + OrderPartitionedRegionName);
        try {
          partitionedregion.localDestroyRegion();
          fail("It should have thrown an Exception saying: " + expectedExMessage);
        } catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Messageee : " + Expected.getMessage());
          assertTrue(Expected.getMessage().contains(expectedExMessage));
        }
      }
    });

    dataStore1.invoke(new CacheSerializableRunnable("PR with Local Destroy") {
      @Override
      public void run2() {
        Region partitionedregion =
            basicGetCache().getRegion(SEPARATOR + CustomerPartitionedRegionName);
        try {
          partitionedregion.localDestroyRegion();
          fail("It should have thrown an Exception saying: " + expectedExMessage);
        } catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Messageee : " + Expected.getMessage());
          assertTrue(Expected.getMessage().contains(expectedExMessage));
        }
      }
    });
    ex.remove();
  }

  @Test
  public void testColocatedPRWithDestroy() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;
    try {
      // Create Customer PartitionedRegion in All VMs
      regionName = CustomerPartitionedRegionName;
      colocatedWith = null;
      isPartitionResolver = Boolean.FALSE;
      attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
          colocatedWith, isPartitionResolver};
      createPartitionedRegion(attributeObjects);

      // Create Order PartitionedRegion in All VMs
      regionName = OrderPartitionedRegionName;
      colocatedWith = CustomerPartitionedRegionName;
      isPartitionResolver = Boolean.FALSE;
      attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
          colocatedWith, isPartitionResolver};
      createPartitionedRegion(attributeObjects);
    } catch (Exception Expected) {
      assertTrue(Expected instanceof IllegalStateException);
    }

    // Put the customer 1-10 in CustomerPartitionedRegion

    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor
        .invoke(() -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName));

    // add expected exception string
    final String expectedExMessage =
        "colocation chain cannot be destroyed, " + "unless all its children";
    final IgnoredException ex = IgnoredException.addIgnoredException(expectedExMessage, dataStore1);
    dataStore1.invoke(new CacheSerializableRunnable("PR with destroy") {
      @Override
      public void run2() {
        Region partitionedregion =
            basicGetCache().getRegion(SEPARATOR + CustomerPartitionedRegionName);
        try {
          partitionedregion.destroyRegion();
          fail("It should have thrown an Exception saying: " + expectedExMessage);
        } catch (IllegalStateException expected) {
          LogWriterUtils.getLogWriter().info("Got message: " + expected.getMessage());
          assertTrue(expected.getMessage().contains(expectedExMessage));
        }
      }
    });
    ex.remove();

    dataStore1.invoke(new CacheSerializableRunnable("PR with destroy") {
      @Override
      public void run2() {
        Region partitionedregion =
            basicGetCache().getRegion(SEPARATOR + OrderPartitionedRegionName);
        try {
          partitionedregion.destroyRegion();
        } catch (Exception unexpected) {
          unexpected.printStackTrace();
          LogWriterUtils.getLogWriter().info("Unexpected Message: " + unexpected.getMessage());
          fail("Could not destroy the child region.");
        }
      }
    });

    dataStore1.invoke(new CacheSerializableRunnable("PR with destroy") {
      @Override
      public void run2() {
        Region partitionedregion =
            basicGetCache().getRegion(SEPARATOR + CustomerPartitionedRegionName);
        try {
          partitionedregion.destroyRegion();
        } catch (Exception unexpected) {
          unexpected.printStackTrace();
          LogWriterUtils.getLogWriter().info("Unexpected Message: " + unexpected.getMessage());
          fail("Could not destroy the parent region.");
        }
      }
    });
  }

  @Test
  public void testColocatedPRWithClose() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 20;

    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Put the customer 1-10 in CustomerPartitionedRegion

    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor
        .invoke(() -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName));


    // Closing region with colocated regions will throw an exception
    // and the region will not be closed.
    accessor.invoke(() -> PRColocationDUnitTest
        .closeRegionWithColocatedRegions(CustomerPartitionedRegionName, false));

    // Destroying region with colocated regions will throw an exception
    // and the region will not be closed.
    accessor.invoke(() -> PRColocationDUnitTest
        .closeRegionWithColocatedRegions(CustomerPartitionedRegionName, true));


    // Closing the colocated regions in the right order should work
    accessor.invoke(() -> PRColocationDUnitTest.closeRegion(OrderPartitionedRegionName));
    accessor.invoke(() -> PRColocationDUnitTest.closeRegion(CustomerPartitionedRegionName));
  }

  /*
   * Test For partition Region with Key Based Routing Resolver
   */
  @Test
  public void testPartitionResolverPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;
    try {
      // Create Customer PartitionedRegion in All VMs
      regionName = CustomerPartitionedRegionName;
      colocatedWith = null;
      isPartitionResolver = Boolean.FALSE;
      attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
          colocatedWith, isPartitionResolver};
      createPartitionedRegion(attributeObjects);

      // Create Order PartitionedRegion in All VMs
      regionName = OrderPartitionedRegionName;
      colocatedWith = CustomerPartitionedRegionName;
      isPartitionResolver = Boolean.FALSE;
      attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
          colocatedWith, isPartitionResolver};
      createPartitionedRegion(attributeObjects);

      // With same Key Based Partition Resolver
      accessor.invoke(new SerializableCallable("Create data, invoke exectuable") {
        @Override
        public Object call() throws Exception {
          PartitionedRegion prForCustomer =
              (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
          assertNotNull(prForCustomer);
          DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(1);
          prForCustomer.put(dummy, 100);
          assertEquals(prForCustomer.get(dummy), 100);
          LogWriterUtils.getLogWriter()
              .info("Key :" + dummy.dummyID + " Value :" + prForCustomer.get(dummy));

          PartitionedRegion prForOrder =
              (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
          assertNotNull(prForOrder);
          prForOrder.put(dummy, 200);
          assertEquals(prForOrder.get(dummy), 200);
          LogWriterUtils.getLogWriter()
              .info("Key :" + dummy.dummyID + " Value :" + prForOrder.get(dummy));
          return null;
        }
      });
    } catch (Exception unexpected) {
      unexpected.printStackTrace();
      LogWriterUtils.getLogWriter().info("Unexpected Message: " + unexpected.getMessage());
      fail("Test failed");
    }
  }

  /*
   * Test added to check the colocation of regions Also checks for the colocation of the buckets
   */
  @Test
  public void testColocationPartitionedRegion() throws Throwable {
    // Create Cache in all VMs VM0,VM1,VM2,VM3

    createCacheInAllVms();

    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Shipment PartitionedRegion in All VMs
    regionName = ShipmentPartitionedRegionName;
    colocatedWith = OrderPartitionedRegionName;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(() -> PRColocationDUnitTest
        .validateBeforePutCustomerPartitionedRegion(CustomerPartitionedRegionName));

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));

    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor
        .invoke(() -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName));

    // Put the shipment 1-10 for each order in ShipmentPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putShipmentPartitionedRegion(ShipmentPartitionedRegionName));

    // for VM0 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore1 = dataStore1
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM1 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore2 = dataStore2
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM3 Datastore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore3 = dataStore3
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // Check the total number of buckets created in all three Vms are equalto 30
    totalNumBucketsInTest = totalBucketsInDataStore1
        + totalBucketsInDataStore2 + totalBucketsInDataStore3;
    assertEquals(totalNumBucketsInTest, 30);

    // This is the importatnt check. Checks that the colocated Customer,Order
    // and Shipment are in the same VM

    accessor.invoke(() -> PRColocationDUnitTest.validateAfterPutPartitionedRegion(
        CustomerPartitionedRegionName, OrderPartitionedRegionName, ShipmentPartitionedRegionName));
  }


  /**
   * Test verifies following invalid collocation creation <br>
   * Member 1: PR2 colocatedWith PR1 <br>
   * Member 2: PR2 is not colocated <br>
   * Should throw IllegalStateException
   *
   */
  @Test
  public void testColocationPartitionedRegionWithNullColocationSpecifiedOnOneNode()
      throws Throwable {
    try {
      createCacheInAllVms();
      getCache().getLogger().info(
          "<ExpectedException action=add>" + "IllegalStateException" + "</ExpectedException>");

      redundancy = 1;
      localMaxmemory = 50;
      totalNumBuckets = 11;

      // Create Customer PartitionedRegion in All VMs
      regionName = CustomerPartitionedRegionName;
      colocatedWith = null;
      isPartitionResolver = Boolean.TRUE;
      attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
          null /* colocatedWith */, isPartitionResolver};
      dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects);
      createPR(regionName, redundancy, localMaxmemory, totalNumBuckets, null /* colocatedWith */,
          isPartitionResolver, false);
      // Create Order PartitionedRegion in All VMs
      regionName = OrderPartitionedRegionName;
      colocatedWith = CustomerPartitionedRegionName;
      isPartitionResolver = Boolean.TRUE;
      attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
          colocatedWith, isPartitionResolver};
      dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects);
      createPR(regionName, redundancy, localMaxmemory, totalNumBuckets, null/* colocatedWith */,
          isPartitionResolver, false);
      fail("test failed due to illgal colocation settings did not thorw expected exception");
    } catch (IllegalStateException expected) {
      // test pass
      assertTrue(expected.getMessage().contains("The colocatedWith="));
    } finally {
      getCache().getLogger().info(
          "<ExpectedException action=remove>" + "IllegalStateException" + "</ExpectedException>");
    }
  }


  /*
   * Test added to check the colocation of regions Also checks for the colocation of the buckets
   * with redundancy specified
   */
  @Test
  public void testColocationPartitionedRegionWithRedundancy() throws Throwable {

    // Create Cache in all VMs VM0,VM1,VM2,VM3
    createCacheInAllVms();

    redundancy = 1;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Customer PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Customer PartitionedRegion in All VMs
    regionName = ShipmentPartitionedRegionName;
    colocatedWith = OrderPartitionedRegionName;
    isPartitionResolver = Boolean.TRUE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(() -> PRColocationDUnitTest
        .validateBeforePutCustomerPartitionedRegion(CustomerPartitionedRegionName));

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putCustomerPartitionedRegion(CustomerPartitionedRegionName));

    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor
        .invoke(() -> PRColocationDUnitTest.putOrderPartitionedRegion(OrderPartitionedRegionName));

    // Put the shipment 1-10 for each order in ShipmentPartitionedRegion
    accessor.invoke(
        () -> PRColocationDUnitTest.putShipmentPartitionedRegion(ShipmentPartitionedRegionName));

    // This is the importatnt check. Checks that the colocated Customer,Order
    // and Shipment are in the same VM
    accessor.invoke(() -> PRColocationDUnitTest.validateAfterPutPartitionedRegion(
        CustomerPartitionedRegionName, OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM0 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore1 = dataStore1
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM1 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore2 = dataStore2
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    // for VM3 Datastore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore3 = dataStore3
        .invoke(() -> PRColocationDUnitTest.validateDataStore(CustomerPartitionedRegionName,
            OrderPartitionedRegionName, ShipmentPartitionedRegionName));

    if (redundancy > 0) {
      // for VM0 DataStore check the number of buckets created and the size of
      // bucket for all partitionedRegion
      dataStore1.invoke(
          () -> PRColocationDUnitTest.validateDataStoreForRedundancy(CustomerPartitionedRegionName,
              OrderPartitionedRegionName, ShipmentPartitionedRegionName));

      // for VM1 DataStore check the number of buckets created and the size of
      // bucket for all partitionedRegion
      dataStore2.invoke(
          () -> PRColocationDUnitTest.validateDataStoreForRedundancy(CustomerPartitionedRegionName,
              OrderPartitionedRegionName, ShipmentPartitionedRegionName));

      // for VM3 Datastore check the number of buckets created and the size of
      // bucket for all partitionedRegion
      dataStore3.invoke(
          () -> PRColocationDUnitTest.validateDataStoreForRedundancy(CustomerPartitionedRegionName,
              OrderPartitionedRegionName, ShipmentPartitionedRegionName));
    }

    // Check the total number of buckets created in all three Vms are equalto 60
    totalNumBucketsInTest = totalBucketsInDataStore1
        + totalBucketsInDataStore2 + totalBucketsInDataStore3;
    assertEquals(totalNumBucketsInTest, 60);
  }

  /**
   * Confirm that the redundancy must be the same for colocated partitioned regions
   *
   */
  @Test
  public void testRedundancyRestriction() throws Exception {
    final String rName = getUniqueName();
    final Integer red0 = 0;
    final Integer red1 = 1;

    CacheSerializableRunnable createPRsWithRed =
        new CacheSerializableRunnable("createPrsWithDifferentRedundancy") {
          @Override
          public void run2() throws CacheException {
            getCache();
            IgnoredException.addIgnoredException("redundancy should be same as the redundancy");
            createPR(rName, red1, 100, 3, null, Boolean.FALSE,
                Boolean.FALSE);
            try {
              createPR(rName + "colo", red0, 100, 3, rName,
                  Boolean.FALSE, Boolean.FALSE);
              fail("Expected different redundancy levels to throw.");
            } catch (IllegalStateException expected) {
              assertEquals(
                  "Current PartitionedRegion's redundancy should be same as the redundancy of colocated PartitionedRegion",
                  expected.getMessage());
            }
          }
        };
    dataStore1.invoke(createPRsWithRed);
  }

  /**
   * Tests to make sure that a VM will not make copies of any buckets for a region until all of the
   * colocated regions are created.
   *
   */
  @Test
  public void testColocatedPRRedundancyRecovery() throws Throwable {
    createCacheInAllVms();
    redundancy = 1;
    localMaxmemory = 50;
    totalNumBuckets = 11;
    // Create Customer PartitionedRegion in Data store 1
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.FALSE;
    Object[] attributeObjects1 = new Object[] {regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver};
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);

    // Create Order PartitionedRegion in Data store 1
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.FALSE;
    Object[] attributeObjects2 = new Object[] {regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver};
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);

    // create a few buckets in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      @Override
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Region region2 = basicGetCache().getRegion(OrderPartitionedRegionName);
        region1.put(1, "A");
        region1.put(2, "A");
        region2.put(1, "A");
        region2.put(2, "A");
      }
    });

    // add a listener for region recovery
    dataStore2.invoke(new SerializableRunnable("Add recovery listener") {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });

    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);

    // Make sure no redundant copies of buckets get created for the first PR in datastore2 because
    // the second PR has not yet been created.
    SerializableRunnable checkForBuckets = new SerializableRunnable("check for buckets") {
      @Override
      public void run() {
        PartitionedRegion region1 =
            (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region1, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        // there should be no buckets on this node, because we don't
        // have all of the colocated regions
        assertEquals(Collections.emptyList(), region1.getLocalBucketsListTestOnly());
        assertEquals(0, region1.getRegionAdvisor().getBucketRedundancy(1));
      }
    };

    dataStore2.invoke(checkForBuckets);

    // create another bucket in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      @Override
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Region region2 = basicGetCache().getRegion(OrderPartitionedRegionName);
        region1.put(3, "A");
        region2.put(3, "A");
      }
    });


    // Make sure that no copies of buckets are created for the first PR in datastore2
    dataStore2.invoke(checkForBuckets);

    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);

    // Now we should get redundant copies of buckets for both PRs
    dataStore2.invoke(new SerializableRunnable("check for bucket creation") {
      @Override
      public void run() {
        PartitionedRegion region1 =
            (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        PartitionedRegion region2 =
            (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region2, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        // we should now have copies all of the buckets
        assertEquals(3, region1.getLocalBucketsListTestOnly().size());
        assertEquals(3, region2.getLocalBucketsListTestOnly().size());
      }
    });
  }

  @Test
  public void testColocationPartitionedRegionWithKeyPartitionResolver() throws Throwable {
    // Create Cache in all VMs VM0,VM1,VM2,VM3

    createCacheInAllVms();

    redundancy = 0;
    localMaxmemory = 50;
    totalNumBuckets = 11;

    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.FALSE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.FALSE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Create Shipment PartitionedRegion in All VMs
    regionName = ShipmentPartitionedRegionName;
    colocatedWith = OrderPartitionedRegionName;
    isPartitionResolver = Boolean.FALSE;
    attributeObjects = new Object[] {regionName, redundancy, localMaxmemory, totalNumBuckets,
        colocatedWith, isPartitionResolver};
    createPartitionedRegion(attributeObjects);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(() -> PRColocationDUnitTest
        .validateBeforePutCustomerPartitionedRegion(CustomerPartitionedRegionName));

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(PRColocationDUnitTest::putData_KeyBasedPartitionResolver);

    accessor.invoke(PRColocationDUnitTest::executeFunction);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.**" + ";org.apache.geode.test.dunit.**"
            + ";org.apache.geode.test.junit.**"
            + ";org.apache.geode.internal.cache.execute.data.CustId"
            + ";org.apache.geode.internal.cache.execute.data.Customer");
    return result;
  }

  @Test
  public void testColocatedPRRedundancyRecovery2() throws Throwable {
    createCacheInAllVms();

    // add a listener for region recovery
    dataStore1.invoke(new SerializableRunnable("Add recovery listener") {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });

    // add a listener for region recovery
    dataStore2.invoke(new SerializableRunnable("Add recovery listener") {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });

    redundancy = 1;
    localMaxmemory = 50;
    totalNumBuckets = 11;
    // Create Customer PartitionedRegion in Data store 1
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.FALSE;
    Object[] attributeObjects1 = new Object[] {regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver};

    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);

    // create a few buckets in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      @Override
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        region1.put(1, "A");
        region1.put(2, "B");
      }
    });

    SerializableRunnable checkForBuckets_ForCustomer =
        new SerializableRunnable("check for buckets") {
          @Override
          public void run() {
            PartitionedRegion region1 =
                (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
            MyResourceObserver observer =
                (MyResourceObserver) InternalResourceManager.getResourceObserver();
            try {
              observer.waitForRegion(region1, 60 * 1000);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            assertEquals(2, region1.getDataStore().getAllLocalBucketIds().size());
            assertEquals(2, region1.getDataStore().getAllLocalPrimaryBucketIds().size());
          }
        };

    dataStore1.invoke(checkForBuckets_ForCustomer);

    // Create Order PartitionedRegion in Data store 1
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.FALSE;
    Object[] attributeObjects2 = new Object[] {regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver};

    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);

    SerializableRunnable checkForBuckets_ForOrder = new SerializableRunnable("check for buckets") {
      @Override
      public void run() {
        PartitionedRegion region =
            (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals(2, region.getDataStore().getAllLocalBucketIds().size());
        assertEquals(2, region.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };
    Wait.pause(5000);
    dataStore1.invoke(checkForBuckets_ForOrder);

    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);

    SerializableRunnable checkForBuckets = new SerializableRunnable("check for buckets") {
      @Override
      public void run() {
        PartitionedRegion region1 =
            (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region1, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals("Unexpected local bucketIds: " + region1.getDataStore().getAllLocalBucketIds(),
            0, region1.getDataStore().getAllLocalBucketIds().size());
        assertEquals(0, region1.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };

    dataStore2.invoke(checkForBuckets);

    // create another bucket in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      @Override
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        region1.put(3, "C");
      }
    });


    // Make sure that no copies of buckets are created for the first PR in datastore2
    dataStore2.invoke(checkForBuckets);

    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);

    // Now we should get redundant copies of buckets for both PRs
    dataStore2.invoke(new SerializableRunnable("check for bucket creation") {
      @Override
      public void run() {
        PartitionedRegion region1 =
            (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        PartitionedRegion region2 =
            (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region2, 1 * 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        // we should now have copies all of the buckets
        assertEquals(3, region1.getLocalBucketsListTestOnly().size());
        assertEquals(3, region2.getLocalBucketsListTestOnly().size());
      }
    });

    dataStore1.invoke(new SerializableRunnable("check for bucket creation") {
      @Override
      public void run() {
        PartitionedRegion region2 =
            (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        assertEquals(3, region2.getLocalBucketsListTestOnly().size());
      }
    });
  }

  /**
   * Test for hang condition observed with the PRHARedundancyProvider.createMissingBuckets code.
   *
   * A parent region is populated with buckets. Then the child region is created simultaneously on
   * several nodes.
   *
   */
  @Test
  public void testSimulaneousChildRegionCreation() throws Throwable {
    createCacheInAllVms();

    // add a listener for region recovery
    dataStore1.invoke(new SerializableRunnable("Add recovery listener") {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });

    // add a listener for region recovery
    dataStore2.invoke(new SerializableRunnable("Add recovery listener") {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });

    redundancy = 1;
    localMaxmemory = 50;
    totalNumBuckets = 60;
    // Create Customer PartitionedRegion in Data store 1
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = Boolean.FALSE;
    Object[] attributeObjects1 = new Object[] {regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver};

    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);

    // create a few buckets in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      @Override
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        for (int i = 0; i < 50; i++) {
          region1.put(i, "A");
        }
      }
    });

    // Create Order PartitionedRegion in Data store 1
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = Boolean.FALSE;
    Object[] attributeObjects2 = new Object[] {regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver};

    AsyncInvocation async1 =
        dataStore1.invokeAsync(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    AsyncInvocation async2 =
        dataStore2.invokeAsync(PRColocationDUnitTest.class, "createPR", attributeObjects2);

    async1.join();
    if (async1.exceptionOccurred()) {
      throw async1.getException();
    }

    async2.join();
    if (async2.exceptionOccurred()) {
      throw async2.getException();
    }

    Wait.pause(5000);
    SerializableRunnable checkForBuckets_ForOrder = new SerializableRunnable("check for buckets") {
      @Override
      public void run() {
        PartitionedRegion region =
            (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer =
            (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals(50, region.getDataStore().getAllLocalBucketIds().size());
        assertEquals(25, region.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };

    dataStore1.invoke(checkForBuckets_ForOrder);
    dataStore2.invoke(checkForBuckets_ForOrder);
  }

  public static void putData_KeyBasedPartitionResolver() {
    PartitionedRegion prForCustomer =
        (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
    assertNotNull(prForCustomer);
    PartitionedRegion prForOrder =
        (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
    assertNotNull(prForOrder);
    PartitionedRegion prForShipment =
        (PartitionedRegion) basicGetCache().getRegion(ShipmentPartitionedRegionName);
    assertNotNull(prForShipment);

    for (int i = 1; i <= 100; i++) {
      DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(i);
      prForCustomer.put(dummy, 1 * i);
      prForOrder.put(dummy, 10 * i);
      prForShipment.put(dummy, 100 * i);
    }
  }

  public static void executeFunction() {

    Function inlineFunction = new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        RegionFunctionContext rfContext = (RegionFunctionContext) context;
        Region r = rfContext.getDataSet();
        if (r.getName().equals(CustomerPartitionedRegionName)) {
          Map map = ColocationHelper.getColocatedLocalDataSetsForBuckets((PartitionedRegion) r,
              new HashSet<>());
          assertEquals(2, map.size());
          rfContext.getResultSender().sendResult(map.size());
          map = ColocationHelper.constructAndGetAllColocatedLocalDataSet((PartitionedRegion) r,
              new int[] {2, 0, 1});
          assertEquals(3, map.size());
          rfContext.getResultSender().lastResult(map.size());
        } else if (r.getName().equals(OrderPartitionedRegionName)) {
          Map map = ColocationHelper.getColocatedLocalDataSetsForBuckets((PartitionedRegion) r,
              new HashSet<>());
          assertEquals(2, map.size());
          rfContext.getResultSender().sendResult(map.size());
          map = ColocationHelper.constructAndGetAllColocatedLocalDataSet((PartitionedRegion) r,
              new int[] {2, 0, 1});
          assertEquals(3, map.size());
          rfContext.getResultSender().lastResult(map.size());
        } else if (r.getName().equals(ShipmentPartitionedRegionName)) {
          Map map = ColocationHelper.getColocatedLocalDataSetsForBuckets((PartitionedRegion) r,
              new HashSet<>());
          assertEquals(2, map.size());
          rfContext.getResultSender().sendResult(map.size());
          map = ColocationHelper.constructAndGetAllColocatedLocalDataSet((PartitionedRegion) r,
              new int[] {2, 0, 1});
          assertEquals(3, map.size());
          rfContext.getResultSender().lastResult(map.size());
        }
      }

      @Override
      public String getId() {
        return "inlineFunction";
      }

      @Override
      public boolean hasResult() {
        return true;
      }

      @Override
      public boolean isHA() {
        return false;
      }

      @Override
      public boolean optimizeForWrite() {
        return false;
      }
    };
    PartitionedRegion prForCustomer =
        (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
    final Set testKeysSet = new HashSet();
    DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(10);
    testKeysSet.add(dummy);
    Execution dataSet = FunctionService.onRegion(prForCustomer);
    ResultCollector rc = dataSet.withFilter(testKeysSet).execute(inlineFunction);
    assertEquals(2, ((List) rc.getResult()).size());

    PartitionedRegion prForOrder =
        (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
    dataSet = FunctionService.onRegion(prForOrder);
    rc = dataSet.withFilter(testKeysSet).execute(inlineFunction);
    assertEquals(2, ((List) rc.getResult()).size());

    PartitionedRegion prForShipment =
        (PartitionedRegion) basicGetCache().getRegion(ShipmentPartitionedRegionName);
    dataSet = FunctionService.onRegion(prForShipment);
    rc = dataSet.withFilter(testKeysSet).execute(inlineFunction);
    assertEquals(2, ((List) rc.getResult()).size());
  }


  public static void validateDataStoreForRedundancy(String customerPartitionedRegionName,
      String orderPartitionedRegionName, String shipmentPartitionedRegionName) {

    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + shipmentPartitionedRegionName);
    } catch (Exception e) {
      fail("validateDataStore : Failed while getting the region from basicGetCache()");
    }
    ArrayList primaryBucketListForCustomer = null;
    ArrayList secondaryBucketListForCustomer = null;
    ArrayList primaryBucketListForOrder = null;
    ArrayList secondaryBucketListForOrder = null;
    ArrayList primaryBucketListForShipment = null;
    ArrayList secondaryBucketListForShipment = null;

    {
      // this is the test for the colocation of the secondaries
      int totalSizeOfBucketsForCustomer =
          customerPartitionedregion.getDataStore().getBucketsManaged();
      int sizeOfPrimaryBucketsForCustomer =
          customerPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged();
      int sizeOfSecondaryBucketsForCustomer =
          totalSizeOfBucketsForCustomer - sizeOfPrimaryBucketsForCustomer;

      primaryBucketListForCustomer =
          (ArrayList) customerPartitionedregion.getDataStore().getLocalPrimaryBucketsListTestOnly();

      secondaryBucketListForCustomer = new ArrayList(sizeOfSecondaryBucketsForCustomer);

      HashMap localBucket2RegionMap =
          (HashMap) customerPartitionedregion.getDataStore().getSizeLocally();
      Set customerEntrySet = localBucket2RegionMap.entrySet();
      assertNotNull(customerEntrySet);
      Iterator customerIterator = customerEntrySet.iterator();
      boolean isSecondary = false;
      while (customerIterator.hasNext()) {
        Map.Entry me = (Map.Entry) customerIterator.next();
        for (final Object o : primaryBucketListForCustomer) {
          if (!me.getKey().equals(o)) {
            isSecondary = true;
          } else {
            isSecondary = false;
            break;
          }
        }
        if (isSecondary) {
          secondaryBucketListForCustomer.add(me.getKey());
        }
      }
      for (final Object value : primaryBucketListForCustomer) {
        LogWriterUtils.getLogWriter().info("Primary Bucket : " + value);

      }
      for (final Object o : secondaryBucketListForCustomer) {
        LogWriterUtils.getLogWriter().info("Secondary Bucket : " + o);
      }
    }
    {
      int totalSizeOfBucketsForOrder = orderPartitionedregion.getDataStore().getBucketsManaged();
      int sizeOfPrimaryBucketsForOrder =
          orderPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged();
      int sizeOfSecondaryBucketsForOrder =
          totalSizeOfBucketsForOrder - sizeOfPrimaryBucketsForOrder;

      primaryBucketListForOrder =
          (ArrayList) orderPartitionedregion.getDataStore().getLocalPrimaryBucketsListTestOnly();

      secondaryBucketListForOrder = new ArrayList(sizeOfSecondaryBucketsForOrder);

      HashMap localBucket2RegionMap =
          (HashMap) customerPartitionedregion.getDataStore().getSizeLocally();
      Set customerEntrySet = localBucket2RegionMap.entrySet();
      assertNotNull(customerEntrySet);
      boolean isSecondary = false;
      for (final Object item : customerEntrySet) {
        Map.Entry me = (Map.Entry) item;
        for (final Object o : primaryBucketListForOrder) {
          if (!me.getKey().equals(o)) {
            isSecondary = true;
          } else {
            isSecondary = false;
            break;
          }
        }
        if (isSecondary) {
          secondaryBucketListForOrder.add(me.getKey());
        }
      }
      for (final Object value : primaryBucketListForOrder) {
        LogWriterUtils.getLogWriter().info("Primary Bucket : " + value);

      }
      for (final Object o : secondaryBucketListForOrder) {
        LogWriterUtils.getLogWriter().info("Secondary Bucket : " + o);
      }
    }
    {
      int totalSizeOfBucketsForShipment =
          shipmentPartitionedregion.getDataStore().getBucketsManaged();
      int sizeOfPrimaryBucketsForShipment =
          shipmentPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged();
      int sizeOfSecondaryBucketsForShipment =
          totalSizeOfBucketsForShipment - sizeOfPrimaryBucketsForShipment;

      primaryBucketListForShipment =
          (ArrayList) shipmentPartitionedregion.getDataStore().getLocalPrimaryBucketsListTestOnly();

      secondaryBucketListForShipment = new ArrayList(sizeOfSecondaryBucketsForShipment);

      HashMap localBucket2RegionMap =
          (HashMap) shipmentPartitionedregion.getDataStore().getSizeLocally();
      Set customerEntrySet = localBucket2RegionMap.entrySet();
      assertNotNull(customerEntrySet);
      Iterator customerIterator = customerEntrySet.iterator();
      boolean isSecondary = false;
      while (customerIterator.hasNext()) {
        Map.Entry me = (Map.Entry) customerIterator.next();
        for (final Object o : primaryBucketListForShipment) {
          if (!me.getKey().equals(o)) {
            isSecondary = true;
          } else {
            isSecondary = false;
            break;
          }
        }
        if (isSecondary) {
          secondaryBucketListForShipment.add(me.getKey());
        }
      }
      for (final Object value : primaryBucketListForShipment) {
        LogWriterUtils.getLogWriter().info("Primary Bucket : " + value);

      }
      for (final Object o : secondaryBucketListForShipment) {
        LogWriterUtils.getLogWriter().info("Secondary Bucket : " + o);
      }
    }

    assertTrue(primaryBucketListForCustomer.containsAll(primaryBucketListForOrder));
    assertTrue(primaryBucketListForCustomer.containsAll(primaryBucketListForShipment));
    assertTrue(primaryBucketListForOrder.containsAll(primaryBucketListForOrder));

    assertTrue(secondaryBucketListForCustomer.containsAll(secondaryBucketListForOrder));
    assertTrue(secondaryBucketListForCustomer.containsAll(secondaryBucketListForShipment));
    assertTrue(secondaryBucketListForOrder.containsAll(secondaryBucketListForOrder));
  }

  public static Integer validateDataStore(String customerPartitionedRegionName,
      String orderPartitionedRegionName, String shipmentPartitionedRegionName) {

    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + shipmentPartitionedRegionName);
    } catch (Exception e) {
      fail("validateDataStore : Failed while getting the region from cache");
    }

    HashMap localBucket2RegionMap =
        (HashMap) customerPartitionedregion.getDataStore().getSizeLocally();
    int customerBucketSize = localBucket2RegionMap.size();
    LogWriterUtils.getLogWriter().info("Size of the " + customerPartitionedRegionName
        + " in this VM :- " + localBucket2RegionMap.size());
    LogWriterUtils.getLogWriter()
        .info("Size of primary buckets the " + customerPartitionedRegionName + " in this VM :- "
            + customerPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged());
    Set customerEntrySet = localBucket2RegionMap.entrySet();
    assertNotNull(customerEntrySet);
    for (final Object item : customerEntrySet) {
      Map.Entry me = (Map.Entry) item;
      Integer size = (Integer) me.getValue();
      assertEquals(1, size.intValue());
      LogWriterUtils.getLogWriter()
          .info("Size of the Bucket " + me.getKey() + ": - " + size);
    }

    // This is the test to check the size of the buckets created
    localBucket2RegionMap = (HashMap) orderPartitionedregion.getDataStore().getSizeLocally();
    int orderBucketSize = localBucket2RegionMap.size();
    LogWriterUtils.getLogWriter().info("Size of the " + orderPartitionedRegionName
        + " in this VM :- " + localBucket2RegionMap.size());
    LogWriterUtils.getLogWriter()
        .info("Size of primary buckets the " + orderPartitionedRegionName + " in this VM :- "
            + orderPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged());

    Set orderEntrySet = localBucket2RegionMap.entrySet();
    assertNotNull(orderEntrySet);
    for (final Object value : orderEntrySet) {
      Map.Entry me = (Map.Entry) value;
      Integer size = (Integer) me.getValue();
      assertEquals(10, size.intValue());
      LogWriterUtils.getLogWriter()
          .info("Size of the Bucket " + me.getKey() + ": - " + size);
    }
    localBucket2RegionMap = (HashMap) shipmentPartitionedregion.getDataStore().getSizeLocally();
    int shipmentBucketSize = localBucket2RegionMap.size();
    LogWriterUtils.getLogWriter().info("Size of the " + shipmentPartitionedRegionName
        + " in this VM :- " + localBucket2RegionMap.size());
    LogWriterUtils.getLogWriter()
        .info("Size of primary buckets the " + shipmentPartitionedRegionName + " in this VM :- "
            + shipmentPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged());
    Set shipmentEntrySet = localBucket2RegionMap.entrySet();
    assertNotNull(shipmentEntrySet);
    for (final Object o : shipmentEntrySet) {
      Map.Entry me = (Map.Entry) o;
      Integer size = (Integer) me.getValue();
      assertEquals(100, size.intValue());
      LogWriterUtils.getLogWriter()
          .info("Size of the Bucket " + me.getKey() + ": - " + size);
    }

    return customerBucketSize + orderBucketSize + shipmentBucketSize;

  }

  public static void validateColocatedRegions(String partitionedRegionName) {
    PartitionedRegion partitionedRegion =
        (PartitionedRegion) basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    Map colocatedRegions;

    colocatedRegions = ColocationHelper.getAllColocationRegions(partitionedRegion);
    if (partitionedRegionName.equals("A")) {
      assertEquals(14, colocatedRegions.size());
    }
    if (partitionedRegionName.equals("D")) {
      assertEquals(4, colocatedRegions.size());
    }
    if (partitionedRegionName.equals("H")) {
      assertEquals(2, colocatedRegions.size());
    }
    if (partitionedRegionName.equals("B")) {
      assertEquals(8, colocatedRegions.size());
    }
    if (partitionedRegionName.equals("K")) {
      assertEquals(3, colocatedRegions.size());
    }
  }

  public static void validateBeforePutCustomerPartitionedRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    PartitionedRegion partitionedregion = null;
    try {
      partitionedregion =
          (PartitionedRegion) basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    } catch (Exception e) {
      Assert.fail(
          "validateBeforePutCustomerPartitionedRegion : Failed while getting the region from cache",
          e);
    }
    assertNotNull(partitionedregion);
    assertTrue(partitionedregion.getRegionAdvisor().getNumProfiles() == 3);
    assertTrue(partitionedregion.getRegionAdvisor().getNumDataStores() == 3);
  }

  public static void validateAfterPutPartitionedRegion(String customerPartitionedRegionName,
      String orderPartitionedRegionName, String shipmentPartitionedRegionName)
      throws ClassNotFoundException {

    assertNotNull(basicGetCache());
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion) basicGetCache()
          .getRegion(SEPARATOR + shipmentPartitionedRegionName);
    } catch (Exception e) {
      Assert.fail("validateAfterPutPartitionedRegion : failed while getting the region", e);
    }
    assertNotNull(customerPartitionedregion);

    for (int i = 1; i <= 10; i++) {
      InternalDistributedMember idmForCustomer = customerPartitionedregion.getBucketPrimary(i);
      InternalDistributedMember idmForOrder = orderPartitionedregion.getBucketPrimary(i);

      InternalDistributedMember idmForShipment = shipmentPartitionedregion.getBucketPrimary(i);

      // take all the keys from the shipmentfor each bucket
      Set customerKey = customerPartitionedregion.getBucketKeys(i);
      assertNotNull(customerKey);
      for (final Object item : customerKey) {
        CustId custId = (CustId) item;
        assertNotNull(customerPartitionedregion.get(custId));
        Set orderKey = orderPartitionedregion.getBucketKeys(i);
        assertNotNull(orderKey);
        for (final Object value : orderKey) {
          OrderId orderId = (OrderId) value;
          // assertNotNull(orderPartitionedregion.get(orderId));

          if (custId.equals(orderId.getCustId())) {
            LogWriterUtils.getLogWriter()
                .info(orderId + "belongs to node " + idmForCustomer + " " + idmForOrder);
            assertEquals(idmForCustomer, idmForOrder);
          }
          Set shipmentKey = shipmentPartitionedregion.getBucketKeys(i);
          assertNotNull(shipmentKey);
          for (final Object o : shipmentKey) {
            ShipmentId shipmentId = (ShipmentId) o;
            // assertNotNull(shipmentPartitionedregion.get(shipmentId));
            if (orderId.equals(shipmentId.getOrderId())) {
              LogWriterUtils.getLogWriter()
                  .info(shipmentId + "belongs to node " + idmForOrder + " " + idmForShipment);
            }
          }
        }
      }
    }

  }

  protected void createCacheInAllVms() {
    dataStore1.invoke(PRColocationDUnitTest::createCacheInVm);
    dataStore2.invoke(PRColocationDUnitTest::createCacheInVm);
    dataStore3.invoke(PRColocationDUnitTest::createCacheInVm);
    accessor.invoke(PRColocationDUnitTest::createCacheInVm);

  }

  public static void putInPartitionedRegion(Region pr) {
    assertNotNull(basicGetCache());

    for (int i = 1; i <= 10; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      try {
        pr.put(custid, customer);
        assertTrue(pr.containsKey(custid));
        assertEquals(customer, pr.get(custid));
      } catch (Exception e) {
        Assert.fail(
            "putInPartitionedRegion : failed while doing put operation in " + pr.getFullPath(), e);
      }
    }
  }

  public static void closeRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    try {
      partitionedregion.close();
    } catch (Exception e) {
      Assert.fail("closeRegion : failed to close region : " + partitionedregion, e);
    }
  }

  public static void closeRegionWithColocatedRegions(String partitionedRegionName,
      boolean destroy) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    boolean exceptionThrown = false;
    try {
      if (destroy) {
        partitionedregion.destroyRegion();
      } else {
        partitionedregion.close();
      }
    } catch (IllegalStateException e) {
      exceptionThrown = true;
    }
    assertTrue("Region should have failed to close. regionName = " + partitionedRegionName,
        exceptionThrown);
  }

  public static void putCustomerPartitionedRegion(String partitionedRegionName) {
    putCustomerPartitionedRegion(partitionedRegionName, 10);
  }

  public static void putCustomerPartitionedRegion(String partitionedRegionName, int numOfRecord) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= numOfRecord; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      try {
        partitionedregion.put(custid, customer);
        assertTrue(partitionedregion.containsKey(custid));
        assertEquals(customer, partitionedregion.get(custid));
      } catch (Exception e) {
        Assert.fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      LogWriterUtils.getLogWriter().info("Customer :- { " + custid + " : " + customer + " }");
    }
  }

  public static void putOrderPartitionedRegion(String partitionedRegionName) {
    putOrderPartitionedRegion(partitionedRegionName, 10);
  }

  public static void putOrderPartitionedRegion(String partitionedRegionName, int numOfCust) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= numOfCust; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        try {
          partitionedregion.put(orderId, order);
          assertTrue(partitionedregion.containsKey(orderId));
          assertEquals(order, partitionedregion.get(orderId));

        } catch (Exception e) {
          Assert.fail(
              "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        LogWriterUtils.getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
  }

  public static void putOrderPartitionedRegion2(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 11; i <= 100; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        try {
          partitionedregion.put(orderId, order);
          assertTrue(partitionedregion.containsKey(orderId));
          assertEquals(order, partitionedregion.get(orderId));

        } catch (Exception e) {
          Assert.fail(
              "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        LogWriterUtils.getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
  }

  public static void putShipmentPartitionedRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 10; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          try {
            partitionedregion.put(shipmentId, shipment);
            assertTrue(partitionedregion.containsKey(shipmentId));
            assertEquals(shipment, partitionedregion.get(shipmentId));
          } catch (Exception e) {
            Assert.fail(
                "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                e);
          }
          LogWriterUtils.getLogWriter()
              .info("Shipment :- { " + shipmentId + " : " + shipment + " }");
        }
      }
    }
  }

  protected void createPartitionedRegion(Object[] attributes) {
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributes);
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributes);
    dataStore3.invoke(PRColocationDUnitTest.class, "createPR", attributes);
    // make Local max memory = o for accessor
    attributes[2] = 0;
    accessor.invoke(PRColocationDUnitTest.class, "createPR", attributes);
  }

  private void createSubPartitionedRegion(Object[] attributes) {
    dataStore1.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
    dataStore2.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
    dataStore3.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
    // make Local max memory = o for accessor
    attributes[2] = 0;
    accessor.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
  }

  private void createPartitionedRegionOnOneVM(Object[] attributes) {
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributes);
  }

  public static void destroyPR(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region pr = basicGetCache().getRegion(partitionedRegionName);
    assertNotNull(pr);
    try {
      LogWriterUtils.getLogWriter().info("Destroying Partitioned Region " + partitionedRegionName);
      pr.destroyRegion();
      fail("Did not get the expected ISE");
    } catch (Exception e) {
      if (!(e instanceof IllegalStateException)) {
        Assert.fail("Expected IllegalStateException, but it's not.", e);
      }
    }
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith,
      Boolean isPartitionResolver) {
    createPR(partitionedRegionName, redundancy, localMaxMemory, totalNumBuckets, colocatedWith,
        isPartitionResolver, Boolean.FALSE);
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith,
      Boolean isPartitionResolver, Boolean concurrencyChecks) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNumBuckets).setColocatedWith((String) colocatedWith);
    if (isPartitionResolver) {
      paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    }
    PartitionAttributes prAttr = paf.create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    attr.setConcurrencyChecksEnabled(concurrencyChecks);
    assertNotNull(basicGetCache());
    Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
    assertNotNull(pr);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + partitionedRegionName + " created Successfully :" + pr);
  }

  public static void createSubPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith,
      Boolean isPartitionResolver) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxMemory)
        .setTotalNumBuckets(totalNumBuckets).setColocatedWith((String) colocatedWith);
    if (isPartitionResolver) {
      paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    }
    PartitionAttributes prAttr = paf.create();
    AttributesFactory attr = new AttributesFactory();
    assertNotNull(basicGetCache());
    Region root = basicGetCache().createRegion("root" + partitionedRegionName, attr.create());
    attr.setPartitionAttributes(prAttr);
    Region pr = root.createSubregion(partitionedRegionName, attr.create());
    assertNotNull(pr);
    LogWriterUtils.getLogWriter()
        .info("Partitioned sub region " + pr.getName() + " created Successfully :" + pr);
    if (localMaxMemory == 0) {
      putInPartitionedRegion(pr);
    }
  }

  public static void createCacheInVm() {
    new PRColocationDUnitTest().getCache();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        InternalResourceManager.setResourceObserver(null);
      }
    });
    InternalResourceManager.setResourceObserver(null);
  }

  private static class MyResourceObserver extends ResourceObserverAdapter {
    Set<Region> recoveredRegions = new HashSet<>();

    @Override
    public void rebalancingOrRecoveryFinished(Region region) {
      synchronized (this) {
        recoveredRegions.add(region);
      }
    }

    public void waitForRegion(Region region, long timeout) throws InterruptedException {
      long start = System.currentTimeMillis();
      synchronized (this) {
        while (!recoveredRegions.contains(region)) {
          long remaining = timeout - (System.currentTimeMillis() - start);
          assertTrue("Timeout waiting for region recovery", remaining > 0);
          wait(remaining);
        }
      }
    }

  }

  static class DummyKeyBasedRoutingResolver implements PartitionResolver, DataSerializable {
    Integer dummyID;

    public DummyKeyBasedRoutingResolver() {}

    public DummyKeyBasedRoutingResolver(int id) {
      dummyID = id;
    }

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return (Serializable) opDetails.getKey();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      dummyID = DataSerializer.readInteger(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(dummyID, out);
    }

    @Override
    public int hashCode() {
      int i = dummyID;
      return i;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof DummyKeyBasedRoutingResolver)) {
        return false;
      }

      DummyKeyBasedRoutingResolver otherDummyID = (DummyKeyBasedRoutingResolver) o;
      return (otherDummyID.dummyID.equals(dummyID));

    }
  }
}
