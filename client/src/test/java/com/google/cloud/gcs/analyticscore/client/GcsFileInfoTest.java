/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class GcsFileInfoTest {

  private static final String TEST_BUCKET = "bucket";

  @Test
  void rootInfo_hasRootUriAndRootItemInfo() {
    GcsFileInfo rootInfo = GcsFileInfo.ROOT_INFO;

    assertThat(rootInfo.getUri()).isEqualTo(GcsFileInfo.GCS_ROOT_URI);
    assertThat(rootInfo.getItemInfo()).isEqualTo(GcsItemInfo.ROOT_INFO);
    assertThat(rootInfo.getAttributes()).isEmpty();
    assertThat(rootInfo.exists()).isTrue();
    assertThat(rootInfo.isDirectory()).isTrue();
  }

  @Test
  void createNotFound_withUri_returnsNotFoundFileInfo() {
    URI uri = URI.create("gs://" + TEST_BUCKET + "/non-existent.txt");

    GcsFileInfo fileInfo = GcsFileInfo.createNotFound(uri);

    assertNotFound(fileInfo, uri);
    assertThat(fileInfo.isDirectory()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("provideItemIdsAndExpectedUris")
  void createNotFound_withItemId_returnsNotFoundFileInfo(GcsItemId itemId, URI expectedUri) {
    GcsFileInfo fileInfo = GcsFileInfo.createNotFound(itemId);

    assertNotFound(fileInfo, expectedUri);
    assertThat(fileInfo.getItemInfo().getItemId()).isEqualTo(itemId);
  }

  private static Stream<Arguments> provideItemIdsAndExpectedUris() {
    return Stream.of(
        Arguments.of(
            GcsItemId.builder()
                .setBucketName(TEST_BUCKET)
                .setObjectName("non-existent.txt")
                .build(),
            URI.create("gs://" + TEST_BUCKET + "/non-existent.txt")),
        Arguments.of(
            GcsItemId.builder().setBucketName(TEST_BUCKET).build(),
            URI.create("gs://" + TEST_BUCKET)),
        Arguments.of(
            GcsItemId.builder()
                .setBucketName(TEST_BUCKET)
                .setObjectName("non-existent.txt")
                .setContentGeneration(1234L)
                .build(),
            URI.create("gs://" + TEST_BUCKET + "/non-existent.txt")),
        Arguments.of(
            GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a b?c#d%20e").build(),
            URI.create("gs://" + TEST_BUCKET + "/a%20b%3Fc%23d%2520e")),
        Arguments.of(GcsItemId.ROOT, GcsFileInfo.GCS_ROOT_URI));
  }

  @ParameterizedTest
  @EnumSource(GcsItemInfo.ItemType.class)
  void isDirectory_existingItemInfo_delegatesToItemInfo(GcsItemInfo.ItemType itemType) {
    GcsItemInfo itemInfo =
        GcsItemInfo.builder()
            .setItemId(GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("item").build())
            .setSize(0L)
            .setItemType(itemType)
            .build();
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setItemInfo(itemInfo)
            .setUri(URI.create("gs://" + TEST_BUCKET + "/item"))
            .setAttributes(ImmutableMap.of())
            .build();

    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.isDirectory()).isEqualTo(itemType != GcsItemInfo.ItemType.OBJECT);
  }

  @Test
  void isDirectory_notFoundItems_reflectsDirectoryStatus() {
    GcsFileInfo missingFile =
        GcsFileInfo.createNotFound(
            GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("file.txt").build());
    GcsFileInfo missingDir =
        GcsFileInfo.createNotFound(
            GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build());
    GcsFileInfo missingBucket =
        GcsFileInfo.createNotFound(GcsItemId.builder().setBucketName(TEST_BUCKET).build());
    GcsFileInfo missingRoot = GcsFileInfo.createNotFound(GcsItemId.ROOT);

    assertThat(missingFile.isDirectory()).isFalse();
    assertThat(missingDir.isDirectory()).isTrue();
    assertThat(missingBucket.isDirectory()).isTrue();
    assertThat(missingRoot.isDirectory()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("provideRoundTripItemIds")
  void createNotFound_roundTripItemIdToUriToItemId_preservesItemIdAndDirectoryStatus(
      GcsItemId itemId) {
    GcsFileInfo fromItemId = GcsFileInfo.createNotFound(itemId);
    GcsFileInfo roundTripped = GcsFileInfo.createNotFound(fromItemId.getUri());

    assertThat(fromItemId.getUri().getRawQuery()).isNull();
    assertThat(fromItemId.getUri().getRawFragment()).isNull();
    assertThat(roundTripped.getItemInfo().getItemId()).isEqualTo(itemId);
    assertThat(roundTripped.isDirectory()).isEqualTo(fromItemId.isDirectory());
    assertThat(roundTripped.isDirectory()).isEqualTo(itemId.isDirectory());
  }

  private static Stream<GcsItemId> provideRoundTripItemIds() {
    return Stream.of(
        GcsItemId.ROOT,
        GcsItemId.builder().setBucketName(TEST_BUCKET).build(),
        GcsItemId.builder().setBucketName("my_bucket.1-2").build(),
        GcsItemId.builder().setBucketName("my_bucket.1-2").setObjectName("a b?c#d%20e").build(),
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("dir/sub dir/file#1?v=2")
            .build(),
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("trailing/").build(),
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("partition=[1,2]/quotes\"<>{}|\\^`/100%#tag?k=v")
            .build(),
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("percent%2Fslash/unicode-é-中文")
            .build());
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://" + TEST_BUCKET, "gs://" + TEST_BUCKET + "/"})
  void createNotFound_withBucketUri_returnsBucketOnlyItemId(String bucketUriString) {
    URI bucketUri = URI.create(bucketUriString);

    GcsFileInfo fileInfo = GcsFileInfo.createNotFound(bucketUri);

    assertNotFound(fileInfo, bucketUri);
    assertThat(fileInfo.getItemInfo().getItemId())
        .isEqualTo(GcsItemId.builder().setBucketName(TEST_BUCKET).build());
    assertThat(fileInfo.isDirectory()).isTrue();
  }

  @Test
  void createNotFound_withPercentEncodedUri_decodesObjectName() {
    URI encodedUri = URI.create("gs://my_bucket/dir%20name/file%3F1%232%2520end%5B0%5D");

    GcsFileInfo fileInfo = GcsFileInfo.createNotFound(encodedUri);

    assertNotFound(fileInfo, encodedUri);
    assertThat(fileInfo.getItemInfo().getItemId())
        .isEqualTo(
            GcsItemId.builder()
                .setBucketName("my_bucket")
                .setObjectName("dir name/file?1#2%20end[0]")
                .build());
    assertThat(fileInfo.isDirectory()).isFalse();
  }

  @Test
  void createNotFound_withCaseInsensitiveScheme_succeeds() {
    GcsFileInfo rootFileInfo = GcsFileInfo.createNotFound(URI.create("GS:/"));
    GcsFileInfo objectFileInfo =
        GcsFileInfo.createNotFound(URI.create("GS://" + TEST_BUCKET + "/dir/"));

    assertThat(rootFileInfo.getItemInfo().getItemId()).isEqualTo(GcsItemId.ROOT);
    assertThat(rootFileInfo.isDirectory()).isTrue();
    assertThat(objectFileInfo.getItemInfo().getItemId())
        .isEqualTo(GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build());
    assertThat(objectFileInfo.isDirectory()).isTrue();
  }

  @Test
  void createNotFound_nullItemId_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GcsFileInfo.createNotFound((GcsItemId) null));

    assertThat(exception).hasMessageThat().isEqualTo("itemId should not be null");
  }

  @Test
  void createNotFound_invalidItemId_throwsIllegalArgumentException() {
    GcsItemId emptyBucketWithObject =
        GcsItemId.builder().setBucketName("").setObjectName("object.txt").build();
    GcsItemId slashInBucket =
        GcsItemId.builder().setBucketName("bad/bucket").setObjectName("object.txt").build();

    assertThrows(
        IllegalArgumentException.class, () -> GcsFileInfo.createNotFound(emptyBucketWithObject));
    assertThrows(IllegalArgumentException.class, () -> GcsFileInfo.createNotFound(slashInBucket));
  }

  @Test
  void createNotFound_nullUri_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> GcsFileInfo.createNotFound((URI) null));

    assertThat(exception).hasMessageThat().isEqualTo("uri should not be null");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://bucket/object",
        "/relative/path",
        "gs:opaque",
        "gs:?query",
        "gs:///",
        "gs:///object",
        "gs:/bucket",
        "gs://bucket/object?query=1",
        "gs://bucket/object?",
        "gs://bucket/object#fragment",
        "gs://bucket/object#",
        "gs://bucket//",
        "gs://bucket//object",
        "gs://bucket/dir//object",
        "gs://bad%2Fbucket/object"
      })
  void createNotFound_invalidUri_throwsIllegalArgumentException(String invalidUri) {
    URI uri = URI.create(invalidUri);

    assertThrows(IllegalArgumentException.class, () -> GcsFileInfo.createNotFound(uri));
  }

  @Test
  void exists_withExistingItemInfo_returnsTrue() {
    GcsItemInfo existingInfo =
        GcsItemInfo.builder()
            .setItemId(
                GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("file.txt").build())
            .setSize(100L)
            .build();
    GcsFileInfo fileInfo =
        GcsFileInfo.builder()
            .setItemInfo(existingInfo)
            .setUri(URI.create("gs://" + TEST_BUCKET + "/file.txt"))
            .setAttributes(ImmutableMap.of())
            .build();

    assertThat(fileInfo.exists()).isTrue();
  }

  private static void assertNotFound(GcsFileInfo fileInfo, URI expectedUri) {
    assertThat(fileInfo.getUri()).isEqualTo(expectedUri);
    assertThat(fileInfo.exists()).isFalse();
    assertThat(fileInfo.getItemInfo().exists()).isFalse();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(-1L);
  }
}
