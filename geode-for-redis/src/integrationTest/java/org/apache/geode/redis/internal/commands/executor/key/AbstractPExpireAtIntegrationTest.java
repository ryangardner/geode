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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractPExpireAtIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String key = "key";
  private static final String value = "value";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void testLevelTearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void errors_GivenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.PEXPIREAT, 2);
  }

  @Test
  public void givenInvalidTimestamp_returnsNotIntegerError() {
    assertThatThrownBy(() -> jedis
        .sendCommand("key", Protocol.Command.PEXPIREAT, "key", "notInteger"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void should_expireKeyAtTimeSpecifiedInMillis() {
    long unixTimeStampInTheNearFuture = (System.currentTimeMillis()) + 5;
    jedis.set(key, value);
    jedis.pexpireAt(key, unixTimeStampInTheNearFuture);

    GeodeAwaitility.await().until(
        () -> jedis.get(key) == null);
  }
}
