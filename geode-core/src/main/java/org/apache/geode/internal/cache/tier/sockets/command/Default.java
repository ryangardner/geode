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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;


public class Default extends BaseCommand {

  @Immutable
  private static final Default singleton = new Default();

  public static Command getCommand() {
    return singleton;
  }

  private Default() {}

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start) throws IOException {
    // requiresResponse = true; NOT NEEDED... ALWAYS SEND ERROR RESPONSE

    logger.fatal("{}: Unknown message type ({}) with tx: {} from {}",
        new Object[] {serverConnection.getName(),
            MessageType.getString(clientMessage.getMessageType()),
            clientMessage.getTransactionId(),
            serverConnection.getSocketString()});
    writeErrorResponse(clientMessage, MessageType.UNKNOWN_MESSAGE_TYPE_ERROR, serverConnection);
    // responded = true; NOT NEEDED... ALWAYS SEND ERROR RESPONSE
  }

}
