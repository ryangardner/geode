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

import java.time.Duration;
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

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LPushDUnitTest {
  public static final int PUSHER_COUNT = 6;
  public static final int PUSH_LIST_SIZE = 3;
  private static MemberVM locator;

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int MINIMUM_ITERATIONS = 10000;
  private static JedisCluster jedis;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    clusterStartUp.startRedisVM(2, locator.getPort());
    clusterStartUp.startRedisVM(3, locator.getPort());
  }

  @Before
  public void testSetup() {
    clusterStartUp.startRedisVM(1, locator.getPort());
    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, redisServerPort), REDIS_CLIENT_TIMEOUT);
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldPushMultipleElementsAtomically()
      throws ExecutionException, InterruptedException {
    AtomicLong runningCount = new AtomicLong(PUSHER_COUNT);

    List<String> listHashtags = makeListHashtags();
    List<String> keys = makeListKeys(listHashtags);
    List<String> elements1 = makeElementList(PUSH_LIST_SIZE, "element1-");
    List<String> elements2 = makeElementList(PUSH_LIST_SIZE, "element2-");
    List<String> elements3 = makeElementList(PUSH_LIST_SIZE, "element3-");

    List<Runnable> taskList = new ArrayList<>();
    taskList.add(() -> lpushPerformAndVerify(keys.get(0), elements1, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(0), elements1, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(1), elements2, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(1), elements2, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(2), elements3, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(2), elements3, runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(0), runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(1), runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(2), runningCount));

    List<Future> futureList = new ArrayList<>();
    for (Runnable task : taskList) {
      futureList.add(executor.runAsync(task));
    }

    for (int i = 0; i < 100 && runningCount.get() > 0; i++) {
      clusterStartUp.moveBucketForKey(listHashtags.get(i % listHashtags.size()));
      GeodeAwaitility.await().during(Duration.ofMillis(200)).until(() -> true);
    }

    for (Future future : futureList) {
      future.get();
    }

    int totalLength = 0;
    Long length;
    for (String key : keys) {
      length = jedis.llen(key);
      assertThat(length).isEqualTo(MINIMUM_ITERATIONS * 2 * PUSH_LIST_SIZE);
      totalLength += length;
    }
    assertThat(totalLength).isEqualTo(MINIMUM_ITERATIONS * PUSHER_COUNT * PUSH_LIST_SIZE);
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

  @Test
  public void shouldDistributeElementsAcrossCluster()
      throws ExecutionException, InterruptedException {
    final int pusherCount = 6;
    final int pushListSize = 3;
    AtomicLong runningCount = new AtomicLong(pusherCount);

    List<String> listHashtags = makeListHashtags();
    List<String> keys = makeListKeys(listHashtags);
    List<String> elements1 = makeElementList(pushListSize, "element1-");
    List<String> elements2 = makeElementList(pushListSize, "element2-");
    List<String> elements3 = makeElementList(pushListSize, "element2-");

    List<Runnable> taskList = new ArrayList<>();
    taskList.add(() -> lpushPerformAndVerify(keys.get(0), elements1, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(0), elements1, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(1), elements2, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(1), elements2, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(2), elements3, runningCount));
    taskList.add(() -> lpushPerformAndVerify(keys.get(2), elements3, runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(0), runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(1), runningCount));
    taskList.add(() -> verifyListLengthCondition(keys.get(2), runningCount));

    List<Future> futureList = new ArrayList<>();
    for (Runnable task : taskList) {
      futureList.add(executor.runAsync(task));
    }

    GeodeAwaitility.await().during(Duration.ofMillis(200)).until(() -> true);
    clusterStartUp.crashVM(1); // kill primary server

    for (Future future : futureList) {
      future.get();
    }

    int totalLength = 0;
    Long length;
    for (String key : keys) {
      length = jedis.llen(key);
      assertThat(length).isGreaterThanOrEqualTo(MINIMUM_ITERATIONS * 2 * pushListSize);
      assertThat(length % 3).isEqualTo(0);
      totalLength += length;
    }
  }

  private List<String> makeListHashtags() {
    List<String> listHashtags = new ArrayList<>();
    listHashtags.add(clusterStartUp.getKeyOnServer("lpush", 1));
    listHashtags.add(clusterStartUp.getKeyOnServer("lpush", 2));
    listHashtags.add(clusterStartUp.getKeyOnServer("lpush", 3));
    return listHashtags;
  }

  private List<String> makeListKeys(List<String> listHashtags) {
    List<String> keys = new ArrayList<>();
    keys.add(makeListKeyWithHashtag(1, listHashtags.get(0)));
    keys.add(makeListKeyWithHashtag(2, listHashtags.get(1)));
    keys.add(makeListKeyWithHashtag(3, listHashtags.get(2)));
    return keys;
  }

  private String makeListKeyWithHashtag(int index, String hashtag) {
    return "{" + hashtag + "}-key-" + index;
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
