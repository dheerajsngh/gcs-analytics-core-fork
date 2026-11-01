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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Weigher;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheCaffeineImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheNoOpImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Manages the caching layer for GCS objects. This class is thread-safe and acts as a registry for
 * various specialized caches (e.g., Parquet footer cache).
 */
public class AnalyticsCacheManager {

  /**
   * TTL for bucket properties cache in minutes. A 10-minute TTL is used because a bucket's
   * Hierarchical Namespace and Rapid (Zonal) storage class configurations are immutable unless the
   * bucket is deleted and re-created.
   */
  private static final long BUCKET_PROPERTIES_CACHE_TTL_MINUTES = 10;

  private final AnalyticsCache<GcsItemId, ByteBuffer> footerCache;
  private final AnalyticsCache<GcsItemId, ByteBuffer> smallObjectCache;
  private final AnalyticsCache<String, BucketProperties> bucketPropertiesCache;

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified options.
   *
   * @param options The configuration options for the caching layer.
   */
  public AnalyticsCacheManager(GcsCacheOptions options) {
    checkNotNull(options, "options cannot be null");
    Weigher<GcsItemId, ByteBuffer> weigher = (key, value) -> value.remaining();
    this.footerCache =
        options.isFooterCacheEnabled()
            ? AnalyticsCacheCaffeineImpl.create(options.getFooterCacheMaxSizeBytes(), weigher)
            : AnalyticsCacheNoOpImpl.getInstance();
    this.smallObjectCache =
        options.isSmallObjectCacheEnabled()
            ? AnalyticsCacheCaffeineImpl.create(options.getSmallObjectCacheMaxSizeBytes(), weigher)
            : AnalyticsCacheNoOpImpl.getInstance();
    this.bucketPropertiesCache =
        AnalyticsCacheCaffeineImpl.createWithTtlOnly(
            BUCKET_PROPERTIES_CACHE_TTL_MINUTES, TimeUnit.MINUTES);
  }

  /**
   * Returns the cached footer for the given {@code itemId}, obtaining it from the {@code
   * footerLoader} if necessary. This method is atomic; the {@code footerLoader} will be applied at
   * most once per itemId during concurrent access.
   *
   * <p>If the {@code footerLoader} throws an exception, it will be propagated to the caller and the
   * result will not be cached.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public ByteBuffer getFooter(GcsItemId itemId, FooterLoader footerLoader) throws IOException {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(footerLoader, "footerLoader cannot be null");

    return footerCache
        .get(itemId, cachedItemId -> footerLoader.load(cachedItemId))
        .asReadOnlyBuffer();
  }

  /**
   * Returns the cached small object for the given {@code itemId}, obtaining it from the {@code
   * smallObjectLoader} if necessary. This method is atomic.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public ByteBuffer getSmallObject(GcsItemId itemId, SmallObjectLoader smallObjectLoader)
      throws IOException {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(smallObjectLoader, "smallObjectLoader cannot be null");

    return smallObjectCache
        .get(itemId, cachedItemId -> smallObjectLoader.load(cachedItemId))
        .asReadOnlyBuffer();
  }

  /** Invalidates the cached footer for the given {@code itemId}. */
  public void invalidateFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    footerCache.invalidate(itemId);
  }

  /** Invalidates the cached small object for the given {@code itemId}. */
  public void invalidateSmallObject(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    smallObjectCache.invalidate(itemId);
  }

  /**
   * Invalidates all cached entries (across all content generations) in the footer and small-object
   * caches matching the given {@code bucketName} and {@code objectName}. Leaves bucket properties
   * untouched.
   *
   * @param bucketName The name of the GCS bucket; must be non-null and non-empty.
   * @param objectName The name of the GCS object; must be non-null.
   */
  public void invalidateObject(String bucketName, String objectName) {
    checkNotNull(bucketName, "bucketName cannot be null");
    checkArgument(!bucketName.isEmpty(), "bucketName cannot be empty");
    checkNotNull(objectName, "objectName cannot be null");

    Optional<String> targetObjectName = Optional.of(objectName);
    Predicate<GcsItemId> matchesObject =
        key ->
            key.getBucketName().equals(bucketName) && targetObjectName.equals(key.getObjectName());
    footerCache.invalidateIf(matchesObject);
    smallObjectCache.invalidateIf(matchesObject);
  }

  /**
   * Invalidates all cached entries (across all content generations) in the footer and small-object
   * caches matching the bucket and object name of {@code itemId}. Leaves bucket properties
   * untouched.
   *
   * @param itemId The GCS object identifier; must represent a GCS object.
   */
  public void invalidateObject(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    checkArgument(itemId.isGcsObject(), "Expected GCS object to be provided. But got: %s", itemId);
    invalidateObject(itemId.getBucketName(), itemId.getObjectName().get());
  }

  /**
   * Invalidates all cached entries (across all content generations) in the footer and small-object
   * caches under the given bucket or object/directory {@code prefix}. Leaves bucket properties
   * untouched.
   *
   * @param prefix The bucket or object/directory prefix to invalidate; must not be null or GCS
   *     root.
   */
  public void invalidatePrefix(GcsItemId prefix) {
    checkNotNull(prefix, "prefix cannot be null");
    checkArgument(!prefix.isRoot(), "Cannot invalidate prefix for GCS root");
    checkArgument(!prefix.getBucketName().isEmpty(), "prefix bucketName cannot be empty");

    String bucketName = prefix.getBucketName();
    Predicate<GcsItemId> matchesPrefix;
    if (prefix.isBucket()) {
      matchesPrefix = key -> key.getBucketName().equals(bucketName);
    } else {
      String raw = prefix.getObjectName().get();
      String dirPrefix = raw.endsWith("/") ? raw : raw + "/";
      String bare = raw.endsWith("/") ? raw.substring(0, raw.length() - 1) : raw;
      matchesPrefix =
          key -> {
            if (!key.getBucketName().equals(bucketName) || key.getObjectName().isEmpty()) {
              return false;
            }
            String obj = key.getObjectName().get();
            return obj.equals(raw)
                || obj.equals(dirPrefix)
                || (!bare.isEmpty() && obj.equals(bare))
                || obj.startsWith(dirPrefix);
          };
    }
    footerCache.invalidateIf(matchesPrefix);
    smallObjectCache.invalidateIf(matchesPrefix);
  }

  /**
   * Returns the cached properties for the given {@code bucketName}, obtaining it from the {@code
   * bucketPropertiesLoader} if necessary. This method is atomic.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public BucketProperties getBucketProperties(
      String bucketName, BucketPropertiesLoader bucketPropertiesLoader) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    checkNotNull(bucketPropertiesLoader, "bucketPropertiesLoader cannot be null");

    return bucketPropertiesCache.get(bucketName, bucketPropertiesLoader::load);
  }

  /** Invalidates the cached properties for the given {@code bucketName}. */
  public void invalidateBucketProperties(String bucketName) {
    checkNotNull(bucketName, "bucketName cannot be null");
    bucketPropertiesCache.invalidate(bucketName);
  }

  /** Returns the cache used for bucket properties. */
  AnalyticsCache<String, BucketProperties> getBucketPropertiesCache() {
    return bucketPropertiesCache;
  }

  /** Invalidates all cached entries. */
  public void invalidateAll() {
    footerCache.invalidateAll();
    smallObjectCache.invalidateAll();
    bucketPropertiesCache.invalidateAll();
  }

  /** A loader for GCS object footers. */
  @FunctionalInterface
  public interface FooterLoader {
    /** Loads the footer for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }

  /** A loader for small GCS objects. */
  @FunctionalInterface
  public interface SmallObjectLoader {
    /** Loads the small object for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }

  /** A loader for GCS bucket properties. */
  @FunctionalInterface
  public interface BucketPropertiesLoader {
    /** Loads the properties for the given {@code bucketName}. */
    BucketProperties load(String bucketName) throws IOException;
  }
}
