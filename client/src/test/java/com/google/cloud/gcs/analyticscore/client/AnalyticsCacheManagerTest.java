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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheManagerTest {

  private static final String BUCKET_NAME = "test-bucket";
  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("o").build();
  private static final ByteBuffer FOOTER = ByteBuffer.wrap(new byte[] {1, 2, 3});

  private AnalyticsCacheManager manager;

  @BeforeEach
  void setUp() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(true).build());
  }

  @Test
  void getFooter_notPresent_computesAndCachesValue() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer footer =
        manager.getFooter(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return FOOTER.duplicate();
            });
    ByteBuffer secondFooter =
        manager.getFooter(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {4, 5, 6});
            });

    assertThat(footer).isEqualTo(FOOTER);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(secondFooter).isEqualTo(FOOTER);
    assertThat(secondFooter).isNotSameInstanceAs(FOOTER);
    assertThat(secondFooter.isReadOnly()).isTrue();
  }

  @Test
  void getFooter_loaderThrowsIOException_rethrowsIOException() {
    assertThrows(
        IOException.class,
        () ->
            manager.getFooter(
                ITEM_ID,
                itemId -> {
                  throw new IOException("test-io-exception");
                }));
  }

  @Test
  void getFooter_cacheDisabled_anyKey_callsLoaderEveryTime() throws IOException {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());
    AtomicInteger callCount = new AtomicInteger(0);
    AnalyticsCacheManager.FooterLoader loader =
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        };

    manager.getFooter(ITEM_ID, loader);
    manager.getFooter(ITEM_ID, loader);

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void invalidateFooter_cacheDisabled_anyKey_succeeds() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateFooter(ITEM_ID);
  }

  @Test
  void invalidateAll_cacheDisabled_anyKey_succeeds() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateAll();
  }

  @Test
  void invalidateSmallObject_present_removesEntry() throws IOException {
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .build();
    manager = new AnalyticsCacheManager(cacheOptions);
    manager.getSmallObject(ITEM_ID, itemId -> FOOTER.duplicate());

    manager.invalidateSmallObject(ITEM_ID);

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getSmallObject(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });

    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateFooter_present_removesEntry() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    manager.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());

    manager.invalidateFooter(ITEM_ID);

    manager.getFooter(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateAll_withEntries_clearsCache() throws IOException {
    GcsItemId itemId2 = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("o2").build();
    manager.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());
    manager.getFooter(itemId2, itemId -> ByteBuffer.wrap(new byte[] {2}));
    manager.getBucketProperties(BUCKET_NAME, bucketName -> BucketProperties.create(true, false));
    AtomicInteger callCount = new AtomicInteger(0);

    manager.invalidateAll();

    manager.getFooter(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    manager.getFooter(
        itemId2,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    manager.getBucketProperties(
        BUCKET_NAME,
        bucketName -> {
          callCount.incrementAndGet();
          return BucketProperties.create(true, false);
        });
    assertThat(callCount.get()).isEqualTo(3);
  }

  @Test
  void getBucketProperties_notPresent_computesAndCachesValue() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    BucketProperties bucketProperties = BucketProperties.create(true, false);

    BucketProperties result1 =
        manager.getBucketProperties(
            BUCKET_NAME,
            bucketName -> {
              callCount.incrementAndGet();
              return bucketProperties;
            });
    BucketProperties result2 =
        manager.getBucketProperties(
            BUCKET_NAME,
            bucketName -> {
              callCount.incrementAndGet();
              return BucketProperties.create(false, false);
            });

    assertThat(result1).isEqualTo(bucketProperties);
    assertThat(result2).isEqualTo(bucketProperties);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void getBucketProperties_loaderThrowsIOException_rethrowsIOException() {
    assertThrows(
        IOException.class,
        () ->
            manager.getBucketProperties(
                BUCKET_NAME,
                bucketName -> {
                  throw new IOException("test-io-exception");
                }));
  }

  @Test
  void invalidateBucketProperties_present_removesEntry() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    manager.getBucketProperties(BUCKET_NAME, bucketName -> BucketProperties.create(true, false));

    manager.invalidateBucketProperties(BUCKET_NAME);
    manager.getBucketProperties(
        BUCKET_NAME,
        bucketName -> {
          callCount.incrementAndGet();
          return BucketProperties.create(true, false);
        });

    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateObject_nullOrEmptyArguments_throwsExpectedException() {
    assertThrows(NullPointerException.class, () -> manager.invalidateObject(null, "obj"));
    assertThrows(IllegalArgumentException.class, () -> manager.invalidateObject("", "obj"));
    assertThrows(NullPointerException.class, () -> manager.invalidateObject(BUCKET_NAME, null));
    assertThrows(NullPointerException.class, () -> manager.invalidateObject(null));
    assertThrows(IllegalArgumentException.class, () -> manager.invalidateObject(GcsItemId.ROOT));
    assertThrows(
        IllegalArgumentException.class,
        () -> manager.invalidateObject(GcsItemId.builder().setBucketName(BUCKET_NAME).build()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.invalidateObject(
                GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("").build()));
  }

  @Test
  void invalidateObject_byBucketAndObject_evictsAllGenerationsAndPreservesOtherEntries()
      throws IOException {
    AnalyticsCacheManager bothCachesManager = createManagerWithFooterAndSmallObjectCaches();
    GcsItemId targetNoGen = itemId(BUCKET_NAME, "dir/file.parquet");
    GcsItemId targetGen1 = itemId(BUCKET_NAME, "dir/file.parquet", 101L);
    GcsItemId targetGen2 = itemId(BUCKET_NAME, "dir/file.parquet", 202L);
    GcsItemId siblingObj = itemId(BUCKET_NAME, "dir/other.parquet", 101L);
    GcsItemId otherBucketObj = itemId("other-bucket", "dir/file.parquet", 101L);
    for (GcsItemId id :
        new GcsItemId[] {targetNoGen, targetGen1, targetGen2, siblingObj, otherBucketObj}) {
      populateBothObjectCaches(bothCachesManager, id);
    }
    bothCachesManager.getBucketProperties(
        BUCKET_NAME, name -> BucketProperties.create(true, false));

    bothCachesManager.invalidateObject(BUCKET_NAME, "dir/file.parquet");

    assertEvictedFromBothObjectCaches(bothCachesManager, targetNoGen);
    assertEvictedFromBothObjectCaches(bothCachesManager, targetGen1);
    assertEvictedFromBothObjectCaches(bothCachesManager, targetGen2);
    assertCachedInBothObjectCaches(bothCachesManager, siblingObj);
    assertCachedInBothObjectCaches(bothCachesManager, otherBucketObj);
    assertBucketPropertiesCached(bothCachesManager, BUCKET_NAME);
  }

  @Test
  void invalidateObject_byItemIdWithGeneration_evictsAllGenerationsOfObject() throws IOException {
    AnalyticsCacheManager bothCachesManager = createManagerWithFooterAndSmallObjectCaches();
    GcsItemId targetNoGen = itemId(BUCKET_NAME, "dir/file.parquet");
    GcsItemId targetGen1 = itemId(BUCKET_NAME, "dir/file.parquet", 101L);
    GcsItemId targetGen2 = itemId(BUCKET_NAME, "dir/file.parquet", 202L);
    populateBothObjectCaches(bothCachesManager, targetNoGen);
    populateBothObjectCaches(bothCachesManager, targetGen1);
    populateBothObjectCaches(bothCachesManager, targetGen2);

    bothCachesManager.invalidateObject(itemId(BUCKET_NAME, "dir/file.parquet", 999L));

    assertEvictedFromBothObjectCaches(bothCachesManager, targetNoGen);
    assertEvictedFromBothObjectCaches(bothCachesManager, targetGen1);
    assertEvictedFromBothObjectCaches(bothCachesManager, targetGen2);
  }

  @Test
  void invalidateObject_cachesDisabled_succeeds() {
    AnalyticsCacheManager disabledManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder()
                .setFooterCacheEnabled(false)
                .setSmallObjectCacheEnabled(false)
                .build());

    disabledManager.invalidateObject(ITEM_ID);
  }

  @Test
  void invalidatePrefix_invalidPrefix_throwsExpectedException() {
    NullPointerException nullException =
        assertThrows(NullPointerException.class, () -> manager.invalidatePrefix(null));
    IllegalArgumentException rootException =
        assertThrows(
            IllegalArgumentException.class, () -> manager.invalidatePrefix(GcsItemId.ROOT));
    IllegalArgumentException emptyBucketException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                manager.invalidatePrefix(
                    GcsItemId.builder().setBucketName("").setObjectName("dir/").build()));

    assertThat(nullException).hasMessageThat().contains("prefix cannot be null");
    assertThat(rootException).hasMessageThat().contains("Cannot invalidate prefix for GCS root");
    assertThat(emptyBucketException).hasMessageThat().contains("bucketName");
  }

  @Test
  void invalidatePrefix_withoutTrailingSlash_evictsMatchingSubtreeAndPreservesSiblingPrefix()
      throws IOException {
    AnalyticsCacheManager bothCachesManager = createManagerWithFooterAndSmallObjectCaches();
    GcsItemId bareObject = itemId(BUCKET_NAME, "dir");
    GcsItemId markerObject = itemId(BUCKET_NAME, "dir/");
    GcsItemId childNoGen = itemId(BUCKET_NAME, "dir/file.parquet");
    GcsItemId childWithGen = itemId(BUCKET_NAME, "dir/file.parquet", 42L);
    GcsItemId nestedChild = itemId(BUCKET_NAME, "dir/sub/file2.parquet", 7L);
    GcsItemId siblingDirChild = itemId(BUCKET_NAME, "dir_other/file.parquet", 42L);
    GcsItemId siblingBare = itemId(BUCKET_NAME, "dir_other");
    GcsItemId otherBucketChild = itemId("other-bucket", "dir/file.parquet");
    for (GcsItemId id :
        new GcsItemId[] {
          bareObject,
          markerObject,
          childNoGen,
          childWithGen,
          nestedChild,
          siblingDirChild,
          siblingBare,
          otherBucketChild
        }) {
      populateBothObjectCaches(bothCachesManager, id);
    }
    bothCachesManager.getBucketProperties(
        BUCKET_NAME, name -> BucketProperties.create(true, false));

    bothCachesManager.invalidatePrefix(itemId(BUCKET_NAME, "dir"));

    assertEvictedFromBothObjectCaches(bothCachesManager, bareObject);
    assertEvictedFromBothObjectCaches(bothCachesManager, markerObject);
    assertEvictedFromBothObjectCaches(bothCachesManager, childNoGen);
    assertEvictedFromBothObjectCaches(bothCachesManager, childWithGen);
    assertEvictedFromBothObjectCaches(bothCachesManager, nestedChild);
    assertCachedInBothObjectCaches(bothCachesManager, siblingDirChild);
    assertCachedInBothObjectCaches(bothCachesManager, siblingBare);
    assertCachedInBothObjectCaches(bothCachesManager, otherBucketChild);
    assertBucketPropertiesCached(bothCachesManager, BUCKET_NAME);
  }

  @Test
  void invalidatePrefix_withTrailingSlash_evictsMatchingSubtreeAndPreservesSiblingPrefix()
      throws IOException {
    AnalyticsCacheManager bothCachesManager = createManagerWithFooterAndSmallObjectCaches();
    GcsItemId bareObject = itemId(BUCKET_NAME, "dir");
    GcsItemId markerObject = itemId(BUCKET_NAME, "dir/");
    GcsItemId child = itemId(BUCKET_NAME, "dir/file.parquet", 42L);
    GcsItemId siblingDirChild = itemId(BUCKET_NAME, "dir_other/file.parquet", 42L);
    for (GcsItemId id : new GcsItemId[] {bareObject, markerObject, child, siblingDirChild}) {
      populateBothObjectCaches(bothCachesManager, id);
    }
    bothCachesManager.getBucketProperties(
        BUCKET_NAME, name -> BucketProperties.create(true, false));

    bothCachesManager.invalidatePrefix(itemId(BUCKET_NAME, "dir/"));

    assertEvictedFromBothObjectCaches(bothCachesManager, bareObject);
    assertEvictedFromBothObjectCaches(bothCachesManager, markerObject);
    assertEvictedFromBothObjectCaches(bothCachesManager, child);
    assertCachedInBothObjectCaches(bothCachesManager, siblingDirChild);
    assertBucketPropertiesCached(bothCachesManager, BUCKET_NAME);
  }

  @Test
  void invalidatePrefix_slashObjectName_doesNotEvictUnprefixedBucketObjects() throws IOException {
    AnalyticsCacheManager bothCachesManager = createManagerWithFooterAndSmallObjectCaches();
    GcsItemId slashObject = itemId(BUCKET_NAME, "/");
    GcsItemId slashChild = itemId(BUCKET_NAME, "/nested.parquet");
    GcsItemId normalObject = itemId(BUCKET_NAME, "normal.parquet");
    populateBothObjectCaches(bothCachesManager, slashObject);
    populateBothObjectCaches(bothCachesManager, slashChild);
    populateBothObjectCaches(bothCachesManager, normalObject);

    bothCachesManager.invalidatePrefix(itemId(BUCKET_NAME, "/"));

    assertEvictedFromBothObjectCaches(bothCachesManager, slashObject);
    assertEvictedFromBothObjectCaches(bothCachesManager, slashChild);
    assertCachedInBothObjectCaches(bothCachesManager, normalObject);
  }

  @Test
  void invalidatePrefix_bucketPrefix_evictsBucketObjectsAndPreservesBucketProperties()
      throws IOException {
    AnalyticsCacheManager bothCachesManager = createManagerWithFooterAndSmallObjectCaches();
    GcsItemId obj1 = itemId(BUCKET_NAME, "a/file1.parquet");
    GcsItemId obj2 = itemId(BUCKET_NAME, "b/file2.parquet", 77L);
    GcsItemId otherBucketObj = itemId("other-bucket", "a/file1.parquet");
    populateBothObjectCaches(bothCachesManager, obj1);
    populateBothObjectCaches(bothCachesManager, obj2);
    populateBothObjectCaches(bothCachesManager, otherBucketObj);
    bothCachesManager.getBucketProperties(
        BUCKET_NAME, name -> BucketProperties.create(true, false));

    bothCachesManager.invalidatePrefix(GcsItemId.builder().setBucketName(BUCKET_NAME).build());

    assertEvictedFromBothObjectCaches(bothCachesManager, obj1);
    assertEvictedFromBothObjectCaches(bothCachesManager, obj2);
    assertCachedInBothObjectCaches(bothCachesManager, otherBucketObj);
    assertBucketPropertiesCached(bothCachesManager, BUCKET_NAME);
  }

  @Test
  void invalidatePrefix_cachesDisabled_succeeds() {
    AnalyticsCacheManager disabledManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder()
                .setFooterCacheEnabled(false)
                .setSmallObjectCacheEnabled(false)
                .build());

    disabledManager.invalidatePrefix(itemId(BUCKET_NAME, "dir/"));
  }

  private static AnalyticsCacheManager createManagerWithFooterAndSmallObjectCaches() {
    return new AnalyticsCacheManager(
        GcsCacheOptions.builder()
            .setFooterCacheEnabled(true)
            .setFooterCacheMaxSizeBytes(10_000)
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(10_000)
            .build());
  }

  private static GcsItemId itemId(String bucketName, String objectName) {
    return GcsItemId.builder().setBucketName(bucketName).setObjectName(objectName).build();
  }

  private static GcsItemId itemId(String bucketName, String objectName, long generation) {
    return GcsItemId.builder()
        .setBucketName(bucketName)
        .setObjectName(objectName)
        .setContentGeneration(generation)
        .build();
  }

  private static void populateBothObjectCaches(AnalyticsCacheManager cacheManager, GcsItemId id)
      throws IOException {
    cacheManager.getFooter(id, ignored -> FOOTER.duplicate());
    cacheManager.getSmallObject(id, ignored -> FOOTER.duplicate());
  }

  private static void assertEvictedFromBothObjectCaches(
      AnalyticsCacheManager cacheManager, GcsItemId id) throws IOException {
    AtomicInteger footerLoads = new AtomicInteger(0);
    AtomicInteger smallObjectLoads = new AtomicInteger(0);
    cacheManager.getFooter(
        id,
        ignored -> {
          footerLoads.incrementAndGet();
          return FOOTER.duplicate();
        });
    cacheManager.getSmallObject(
        id,
        ignored -> {
          smallObjectLoads.incrementAndGet();
          return FOOTER.duplicate();
        });
    assertThat(footerLoads.get()).isEqualTo(1);
    assertThat(smallObjectLoads.get()).isEqualTo(1);
  }

  private static void assertCachedInBothObjectCaches(
      AnalyticsCacheManager cacheManager, GcsItemId id) throws IOException {
    AtomicInteger footerLoads = new AtomicInteger(0);
    AtomicInteger smallObjectLoads = new AtomicInteger(0);
    cacheManager.getFooter(
        id,
        ignored -> {
          footerLoads.incrementAndGet();
          return FOOTER.duplicate();
        });
    cacheManager.getSmallObject(
        id,
        ignored -> {
          smallObjectLoads.incrementAndGet();
          return FOOTER.duplicate();
        });
    assertThat(footerLoads.get()).isEqualTo(0);
    assertThat(smallObjectLoads.get()).isEqualTo(0);
  }

  private static void assertBucketPropertiesCached(
      AnalyticsCacheManager cacheManager, String bucketName) throws IOException {
    AtomicInteger loads = new AtomicInteger(0);
    cacheManager.getBucketProperties(
        bucketName,
        ignored -> {
          loads.incrementAndGet();
          return BucketProperties.create(false, false);
        });
    assertThat(loads.get()).isEqualTo(0);
  }
}
