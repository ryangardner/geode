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
 *
 */


package org.apache.geode.redis.internal.data;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class PartitionedRegionStatsUpdateTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUpRule = new RedisClusterStartupRule(3);

  private static MemberVM server1;
  private static MemberVM server2;

  private static JedisCluster jedis;

  private static final String STRING_KEY = "{tag1}string key";
  private static final String SET_KEY = "{tag1}set key";
  private static final String HASH_KEY = "{tag1}hash key";
  private static final String LONG_APPEND_VALUE = String.valueOf(Integer.MAX_VALUE);
  private static final String FIELD = "field";

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUpRule.startLocatorVM(0);
    int locatorPort = locator.getPort();

    server1 = clusterStartUpRule.startRedisVM(1, locatorPort);
    server2 = clusterStartUpRule.startRedisVM(2, locatorPort);

    int redisServerPort1 = clusterStartUpRule.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort1), REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void cleanup() {
    jedis.close();
  }

  @Before
  public void setup() {
    clusterStartUpRule.flushAll();
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenStringValueSizeIncreases() {
    String LONG_APPEND_VALUE = String.valueOf(Integer.MAX_VALUE);
    jedis.set(STRING_KEY, "value");

    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis.append(STRING_KEY, LONG_APPEND_VALUE);
    }

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse1).isGreaterThan(initialDataStoreBytesInUse1);
    long server2finalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);
    assertThat(server2finalDataStoreBytesInUse).isEqualTo(finalDataStoreBytesInUse1);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenStringValueDeleted() {
    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long initialDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    jedis.set(STRING_KEY, "value");

    long intermediateDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long intermediateDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);
    assertThat(intermediateDataStoreBytesInUse1).isGreaterThan(initialDataStoreBytesInUse1);
    assertThat(intermediateDataStoreBytesInUse2).isGreaterThan(initialDataStoreBytesInUse2);

    jedis.del(STRING_KEY);

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long finalDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse1).isEqualTo(initialDataStoreBytesInUse1);
    assertThat(finalDataStoreBytesInUse2).isEqualTo(initialDataStoreBytesInUse2);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenStringValueShortened() {
    jedis.set(STRING_KEY, "longer value");
    // The first set of a key stores a serialized blob on the secondary.
    // Any set after that stores a deserialized RedisString on the secondary.
    // So we do an extra set here so that the size will be consistent between thw two calls.
    // Note: we don't know which server will be the primary and which the secondary.
    jedis.set(STRING_KEY, "longer value");

    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long initialDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    jedis.set(STRING_KEY, "value");

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long finalDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse1).isLessThan(initialDataStoreBytesInUse1);
    assertThat(finalDataStoreBytesInUse2).isLessThan(initialDataStoreBytesInUse2);
  }

  @Test
  public void should_resetMemoryUsage_givenFlushAllCommand() {
    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long initialDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(initialDataStoreBytesInUse1).isEqualTo(0L);
    assertThat(initialDataStoreBytesInUse2).isEqualTo(0L);

    jedis.set(STRING_KEY, "value");

    clusterStartUpRule.flushAll();

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long finalDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse1).isEqualTo(initialDataStoreBytesInUse1);
    assertThat(finalDataStoreBytesInUse2).isEqualTo(initialDataStoreBytesInUse2);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenStringValueSizeDoesNotIncrease() {
    jedis.set(STRING_KEY, "value");
    // The first set of a key stores a serialized blob on the secondary.
    // Any set after that stores a deserialized RedisString on the secondary.
    // So we do an extra set here so that the size will be consistent between thw two calls.
    // Note: we don't know which server will be the primary and which the secondary.
    jedis.set(STRING_KEY, "value");

    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long initialDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 1000; i++) {
      jedis.set(STRING_KEY, "value");
    }

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long finalDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse1).isEqualTo(initialDataStoreBytesInUse1);
    assertThat(finalDataStoreBytesInUse2).isEqualTo(initialDataStoreBytesInUse2);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenSetValueSizeIncreases() {
    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long initialDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 1000; i++) {
      jedis.sadd(SET_KEY, "value" + i);
    }

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long finalDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse1).isGreaterThan(initialDataStoreBytesInUse1);
    assertThat(finalDataStoreBytesInUse2).isGreaterThan(initialDataStoreBytesInUse2);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenSetValueSizeDoesNotIncrease() {
    jedis.sadd(SET_KEY, "value");

    long initialDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long initialDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 1000; i++) {
      jedis.sadd(SET_KEY, "value");
    }

    assertThat(jedis.scard(SET_KEY)).isEqualTo(1);

    long finalDataStoreBytesInUse1 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long finalDataStoreBytesInUse2 =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse1).isEqualTo(initialDataStoreBytesInUse1);
    assertThat(finalDataStoreBytesInUse2).isEqualTo(initialDataStoreBytesInUse2);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenSetValueDeleted() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    jedis.sadd(SET_KEY, "value");

    long intermediateDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    assertThat(intermediateDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);

    jedis.del(SET_KEY);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenSetValueSizeDecreases() {
    for (int i = 0; i < 10; i++) {
      jedis.sadd(SET_KEY, "value" + i);
    }

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 10; i++) {
      jedis.srem(SET_KEY, "value" + i);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isLessThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenHashValueSizeIncreases() {
    jedis.hset(HASH_KEY, FIELD, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis.hset(HASH_KEY, FIELD + i, LONG_APPEND_VALUE);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenHashValueDeleted() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    jedis.hset(HASH_KEY, FIELD, "value");

    long intermediateDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    assertThat(intermediateDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);

    jedis.del(HASH_KEY);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenHSetDoesNotIncreaseHashSize() {
    jedis.hset(HASH_KEY, FIELD, "initialvalue"); // two hsets are required to force
    jedis.hset(HASH_KEY, FIELD, "value"); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 10; i++) {
      jedis.hset(HASH_KEY, FIELD, "value");
    }

    assertThat(jedis.hgetAll(HASH_KEY).size()).isEqualTo(1);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenHSetNXIncreasesHashSize() {
    jedis.hset(HASH_KEY, FIELD, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis.hsetnx(HASH_KEY, FIELD + i, "value");
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenHSetNXDoesNotIncreaseHashSize() {
    jedis.hset(HASH_KEY, FIELD, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis.hsetnx(HASH_KEY, FIELD, "value");
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  /******* confirm that the other member agrees upon size *******/

  @Test
  public void should_showMembersAgreeUponUsedHashMemory_afterDeltaPropagation() {
    jedis.hset(HASH_KEY, FIELD, "initialvalue"); // two hsets are required to force
    jedis.hset(HASH_KEY, FIELD, "finalvalue"); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 10; i++) {
      jedis.hset(HASH_KEY, FIELD, "finalvalue");
    }

    assertThat(jedis.hgetAll(HASH_KEY).size()).isEqualTo(1);

    long server2FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);
    long server1FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(server1FinalDataStoreBytesInUse)
        .isEqualTo(server2FinalDataStoreBytesInUse)
        .isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showMembersAgreeUponUsedSetMemory_afterDeltaPropagationWhenAddingMembers() {
    jedis.sadd(SET_KEY, "other"); // two sadds are required to force
    jedis.sadd(SET_KEY, "value"); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 10; i++) {
      jedis.sadd(SET_KEY, "value");
    }

    assertThat(jedis.scard(SET_KEY)).isEqualTo(2);

    long server1FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(server1FinalDataStoreBytesInUse)
        .isEqualTo(server2FinalDataStoreBytesInUse)
        .isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showMembersAgreeUponUsedSetMemory_afterDeltaPropagationWhenRemovingMembers() {
    String value1 = "value1";
    String value2 = "value2";
    jedis.sadd(SET_KEY, value1); // two sadds are required to force
    jedis.sadd(SET_KEY, value2); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    jedis.sadd(SET_KEY, "value3");

    jedis.srem(SET_KEY, value1, value2);

    assertThat(jedis.scard(SET_KEY)).isEqualTo(1);

    long server1DataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2DataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(server1DataStoreBytesInUse).isEqualTo(server2DataStoreBytesInUse);
  }

  @Test
  public void bothPrimaryAndSecondaryChangeWhenRedisStringAddedAndRemoved() {
    long server1SizeBeforeSet =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2SizeBeforeSet =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    jedis.set(STRING_KEY, "value");

    long server1SizeAfterSet =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2SizeAfterSet =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    jedis.del(STRING_KEY);

    long server1SizeAfterDel =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2SizeAfterDel =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(server1SizeAfterSet)
        .isGreaterThan(server1SizeBeforeSet);
    assertThat(server2SizeAfterSet)
        .isGreaterThan(server2SizeBeforeSet);

    assertThat(server1SizeAfterDel)
        .isEqualTo(server1SizeBeforeSet);
    assertThat(server2SizeAfterDel)
        .isEqualTo(server2SizeBeforeSet);
  }
}
