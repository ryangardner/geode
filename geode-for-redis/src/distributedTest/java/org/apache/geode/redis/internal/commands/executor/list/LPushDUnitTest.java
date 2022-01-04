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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class LPushDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  private static final int LIST_SIZE = 1000;
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
  public void shouldDistributeDataAmongCluster() {
    String key = "key";

    List<String> elements = makeElementList(LIST_SIZE, "element1-");

    jedis.lpush(key, elements.toArray(new String[] {}));

    List<String> result = getAllElements(key);

    assertThat(result.toArray()).containsExactlyInAnyOrder(elements.toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingDifferentDataToSameList() {
    String key = "key";

    List<String> elements1 = makeElementList(LIST_SIZE, "element1-");
    List<String> elements2 = makeElementList(LIST_SIZE, "element2-");

    List<String> allElements = new ArrayList<>();
    allElements.addAll(elements1);
    allElements.addAll(elements2);

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis.lpush(key, elements1.get(i)),
        (i) -> jedis.lpush(key, elements2.get(i))).runInLockstep();

    List<String> results = getAllElements(key);

    assertThat(results.toArray()).containsExactlyInAnyOrder(allElements.toArray());
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyAddingDifferentLists() {
    String key1 = "key1";
    String key2 = "key2";

    List<String> elements1 = makeElementList(LIST_SIZE, "element1-");
    List<String> elements2 = makeElementList(LIST_SIZE, "element2-");

    new ConcurrentLoopingThreads(LIST_SIZE,
        (i) -> jedis.lpush(key1, elements1.get(i)),
        (i) -> jedis.lpush(key2, elements2.get(i))).runInLockstep();

    List<String> results1 = getAllElements(key1);
    List<String> results2 = getAllElements(key2);

    assertThat(results1.toArray()).containsExactlyInAnyOrder(elements1.toArray());
    assertThat(results2.toArray()).containsExactlyInAnyOrder(elements2.toArray());

  }

  private List<String> makeElementList(int setSize, String baseString) {
    List<String> elements = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      elements.add(baseString + i);
    }
    return elements;
  }

  private List<String> getAllElements(String key) {
    List<String> elements = new ArrayList<>();
    String element;
    while ((element = jedis.lpop(key)) != null) {
      elements.add(element);
    }
    return elements;
  }
}
