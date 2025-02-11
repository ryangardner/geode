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
package org.apache.geode.redis.internal.commands.executor.set;


import static org.apache.geode.redis.internal.RedisConstants.ERROR_VALUE_MUST_BE_POSITIVE;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.util.Collections;
import java.util.List;

import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.services.RegionProvider;

public class SPopExecutor extends SetRandomExecutor {

  @Override
  protected List<byte[]> performCommand(int count, RegionProvider regionProvider, RedisKey key) {
    if (count < 0) {
      throw new RedisException(getError());
    }

    RedisSet set =
        regionProvider.getTypedRedisData(REDIS_SET, key, false);
    if (count == 0 || set.isNull()) {
      return Collections.emptyList();
    }

    return set.spop(count, regionProvider.getDataRegion(), key);
  }

  @Override
  protected String getError() {
    return ERROR_VALUE_MUST_BE_POSITIVE;
  }
}
