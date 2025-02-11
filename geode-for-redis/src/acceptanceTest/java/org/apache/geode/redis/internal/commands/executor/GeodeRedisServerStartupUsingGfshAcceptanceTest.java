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

package org.apache.geode.redis.internal.commands.executor;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class GeodeRedisServerStartupUsingGfshAcceptanceTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void shouldReturnErrorMessage_givenSamePortAndAddress() throws IOException {

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "same-port-and-address-server",
        "--J=-Dgemfire.geode-for-redis-enabled=true",
        "--J=-Dgemfire.geode-for-redis-bind-address=localhost",
        "--J=-Dgemfire.geode-for-redis-port=" + port);
    GfshExecution execution;

    try (ServerSocket interferingSocket = new ServerSocket()) {
      interferingSocket.bind(new InetSocketAddress("localhost", port));
      execution = GfshScript.of(startServerCommand)
          .expectFailure()
          .execute(gfshRule);
    }

    assertThat(execution.getOutputText()).containsIgnoringCase("address already in use");
  }

  @Test
  public void shouldReturnErrorMessage_givenSamePortAndAllAddresses() throws IOException {

    int port = AvailablePortHelper.getRandomAvailableTCPPort();

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "same-port-all-addresses-server",
        "--J=-Dgemfire.geode-for-redis-enabled=true",
        "--J=-Dgemfire.geode-for-redis-port=" + port);
    GfshExecution execution;

    try (ServerSocket interferingSocket = new ServerSocket()) {
      interferingSocket.bind(new InetSocketAddress("0.0.0.0", port));
      execution = GfshScript.of(startServerCommand)
          .expectFailure()
          .execute(gfshRule);
    }

    assertThat(execution.getOutputText()).containsIgnoringCase("address already in use");
  }

  @Test
  public void shouldReturnErrorMessage_givenInvalidBindAddress() {

    String startServerCommand = String.join(" ",
        "start server",
        "--server-port", "0",
        "--name", "invalid-bind-server",
        "--J=-Dgemfire.geode-for-redis-enabled=true",
        "--J=-Dgemfire.geode-for-redis-bind-address=1.1.1.1");
    GfshExecution execution;

    execution = GfshScript.of(startServerCommand)
        .expectFailure()
        .execute(gfshRule);

    assertThat(execution.getOutputText()).containsIgnoringCase(
        "The geode-for-redis-bind-address 1.1.1.1 is not a valid address for this machine");
  }

  @Test
  public void gfshStartsRedisServer_whenRedisEnabled() {
    String command = "start server --server-port=0 "
        + "--J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_ENABLED + "=true";
    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, 6379)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenCustomPort() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String command = "start server --server-port=0 "
        + "--J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_ENABLED + "=true"
        + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_PORT + "=" + port;

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshStartsRedisServer_whenCustomPortAndBindAddress() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();
    String command = "start server --server-port=0 "
        + "--J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_ENABLED + "=true"
        + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_PORT + "=" + port
        + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_BIND_ADDRESS + "="
        + anyLocal;

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(anyLocal, port)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void gfshDoesNotStartRedisServer_whenNotRedisEnabled() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    String anyLocal = LocalHostUtil.getAnyLocalAddress().getHostAddress();
    String command = "start server --server-port=0 "
        + "--J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_PORT + "=" + port
        + " --J=-Dgemfire." + ConfigurationProperties.GEODE_FOR_REDIS_BIND_ADDRESS + "="
        + anyLocal;

    gfshRule.execute(command);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port)) {
      assertThatThrownBy(() -> jedis.ping()).isInstanceOf(JedisConnectionException.class);
    }
  }

}
