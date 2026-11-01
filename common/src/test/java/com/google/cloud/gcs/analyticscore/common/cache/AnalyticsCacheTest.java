/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.common.cache;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class AnalyticsCacheTest {

  @Test
  void invalidateIf_defaultImplementation_delegatesToInvalidateAll() {
    AnalyticsCache<String, String> customCache = createMinimalCache();
    customCache.put("dir/a", "val1");
    customCache.put("other/b", "val2");

    customCache.invalidateIf(key -> key.startsWith("dir/"));

    assertThat(customCache.get("dir/a")).isEmpty();
    assertThat(customCache.get("other/b")).isEmpty();
    assertThat(customCache.size()).isEqualTo(0);
  }

  @Test
  void invalidateIf_defaultImplementationWithNullPredicate_throwsNullPointerException() {
    AnalyticsCache<String, String> customCache = createMinimalCache();
    customCache.put("key1", "val1");

    NullPointerException exception =
        assertThrows(NullPointerException.class, () -> customCache.invalidateIf(null));

    assertThat(exception).hasMessageThat().contains("predicate cannot be null");
    assertThat(customCache.get("key1")).hasValue("val1");
  }

  @Test
  void cleanUp_defaultImplementation_doesNotEvictEntries() {
    AnalyticsCache<String, String> customCache = createMinimalCache();
    customCache.put("key1", "val1");

    customCache.cleanUp();

    assertThat(customCache.get("key1")).hasValue("val1");
  }

  private static AnalyticsCache<String, String> createMinimalCache() {
    Map<String, String> backing = new ConcurrentHashMap<>();
    return new AnalyticsCache<>() {
      @Override
      public Optional<String> get(String key) {
        return Optional.ofNullable(backing.get(key));
      }

      @Override
      public <E extends Exception> String get(
          String key, ThrowingFunction<? super String, ? extends String, E> mappingFunction)
          throws E {
        String existing = backing.get(key);
        if (existing != null) {
          return existing;
        }
        String computed = mappingFunction.apply(key);
        backing.put(key, computed);
        return computed;
      }

      @Override
      public void put(String key, String value) {
        backing.put(key, value);
      }

      @Override
      public void invalidate(String key) {
        backing.remove(key);
      }

      @Override
      public void invalidateAll() {
        backing.clear();
      }

      @Override
      public long size() {
        return backing.size();
      }
    };
  }
}
