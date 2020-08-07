/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jcustenborder.kafka.common.cache;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;

import java.util.function.Function;


public class XSynchronizedCache<K, V> extends SynchronizedCache<K, V> {

  XSynchronizedCache(Cache<K, V> underlying) {
    super(underlying);
  }

  public V computeIfAbsent(K key,
                           Function<? super K, ? extends V> mappingFunction) {
    if (this.get(key) == null) {
      V newValue = mappingFunction.apply(key);
      if (newValue != null)
        this.put(key, newValue);
    }

    return this.get(key);
  }
}
