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

package org.apache.geode.redis.internal.commands.executor.string;


import org.apache.geode.redis.internal.commands.executor.BaseSetOptions;

/**
 * Class representing different options that can be used with Redis string SET command.
 */
public class SetOptions extends BaseSetOptions {

  private final long expirationMillis;
  private final boolean keepTTL;
  private final boolean inTransaction;

  public SetOptions(Exists exists, long expiration, boolean keepTTL) {
    this(exists, expiration, keepTTL, false);
  }

  public SetOptions(Exists exists, long expiration, boolean keepTTL, boolean inTransaction) {
    super(exists);
    expirationMillis = expiration;
    this.keepTTL = keepTTL;
    this.inTransaction = inTransaction;
  }

  public long getExpiration() {
    return expirationMillis;
  }

  public boolean isKeepTTL() {
    return keepTTL;
  }

  public boolean inTransaction() {
    return inTransaction;
  }
}
