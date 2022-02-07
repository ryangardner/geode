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

package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LPushDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int MINIMUM_ITERATIONS = 10000;
  private static JedisCluster jedis;

  @BeforeClass
  public static void classSetup() {
    MemberVM locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(1, locator.getPort());
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());

    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), REDIS_CLIENT_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void lpush_ShouldPushMultipleElementsAtomically()
      throws ExecutionException, InterruptedException {
    final int pusherCount = 2;
    final int pushListSize = 3;
    AtomicLong runningCount = new AtomicLong(pusherCount);
    String key = "key";
    List<String> elements1 = makeElementList(pushListSize, "element1-");
    List<String> elements2 = makeElementList(pushListSize, "element2-");

    Runnable task1 =
        () -> lpushPerformAndVerify(key, elements1, runningCount);
    Runnable task2 =
        () -> lpushPerformAndVerify(key, elements2, runningCount);
    Runnable task3 =
        () -> verifyListLengthCondition(key, runningCount);

    Future<Void> future1 = executor.runAsync(task1);
    Future<Void> future2 = executor.runAsync(task2);
    Future<Void> future3 = executor.runAsync(task3);

    future1.get();
    future2.get();
    future3.get();

    assertThat(jedis.llen(key)).isEqualTo(MINIMUM_ITERATIONS * pusherCount * pushListSize);
  }

  private void lpushPerformAndVerify(String key, List<String> elementList,
      AtomicLong runningCount) {
    for (int i = 0; i < MINIMUM_ITERATIONS; i++) {
      long listLength = jedis.llen(key);
      long newLength = jedis.lpush(key, elementList.toArray(new String[] {}));
      assertThat(newLength - listLength).isGreaterThanOrEqualTo(3);
      assertThat((newLength - listLength) % 3).isEqualTo(0);
    }
    runningCount.decrementAndGet();
  }

  private void verifyListLengthCondition(String key, AtomicLong runningCount) {
    while (runningCount.get() > 0) {
      assertThat(jedis.llen(key) % 3).isEqualTo(0);
    }
  }

  private List<String> makeElementList(int listSize, String baseString) {
    List<String> elements = new ArrayList<>();
    for (int i = 0; i < listSize; i++) {
      elements.add(baseString + i);
    }
    return elements;
  }
}
