/*
 * Copyright 2025 Google LLC
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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class UriUtilTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String TEST_PATH = "dir/subdir";

  @Test
  void getItemIdFromUri_validUri_succeeds() {
    URI uri = URI.create("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);

    GcsItemId itemId = UriUtil.getItemIdFromUri(uri);

    assertThat(itemId.getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(itemId.getObjectName()).hasValue(TEST_OBJECT);
  }

  @Test
  void getItemIdFromUri_nullUri_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> UriUtil.getItemIdFromUri(null));
  }

  @Test
  void getItemIdFromUri_percentEncodedPath_returnsDecodedObjectName() {
    URI uri = URI.create("gs://" + TEST_BUCKET + "/dir/a%20b%23c%3Fd%25e.txt");

    GcsItemId itemId = UriUtil.getItemIdFromUri(uri);

    assertThat(itemId.getObjectName()).hasValue("dir/a b#c?d%e.txt");
  }

  @Test
  void getItemIdFromUri_bucketNameWithUnderscore_returnsBucketName() {
    // URI.getHost() is null for such a bucket, because '_' is not legal in a host name.
    URI uri = URI.create("gs://my_bucket/" + TEST_OBJECT);

    GcsItemId itemId = UriUtil.getItemIdFromUri(uri);

    assertThat(itemId.getBucketName()).isEqualTo("my_bucket");
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://" + TEST_BUCKET, "gs://" + TEST_BUCKET + "/"})
  void getItemIdFromUri_gcsBucket_returnsBucketItemId(String bucketUri) {
    GcsItemId itemId = UriUtil.getItemIdFromUri(URI.create(bucketUri));

    assertThat(itemId).isEqualTo(GcsItemId.builder().setBucketName(TEST_BUCKET).build());
  }

  @Test
  void getItemIdFromUri_rootUri_returnsRootItemId() {
    GcsItemId itemId = UriUtil.getItemIdFromUri(URI.create("gs:/"));

    assertThat(itemId).isEqualTo(GcsItemId.ROOT);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gs://test-bucket/a?b.txt",
        "gs://test-bucket/a#b.txt",
        "gs://test-bucket/a?",
        "gs://test-bucket/a#",
        "gs://test-bucket?b",
        "gs:/#b"
      })
  void getItemIdFromUri_queryOrFragment_throwsIllegalArgumentException(
      String uriWithQueryOrFragment) {
    URI uri = URI.create(uriWithQueryOrFragment);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromUri(uri));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "GCS URI must not have a query or a fragment; percent-encode '?' as %3F and '#' as %23"
                + " in an object name: "
                + uriWithQueryOrFragment);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://test-bucket/test-object",
        "GS://test-bucket/test-object",
        "//test-bucket/test-object",
        "gs:///",
        "gs:///test-object",
        "gs:/test-bucket",
        "gs:test-bucket",
        ""
      })
  void getItemIdFromUri_invalidUri_throwsIllegalArgumentException(String invalidUri) {
    URI uri = URI.create(invalidUri);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromUri(uri));

    assertThat(e).hasMessageThat().isEqualTo("Invalid GCS path: " + invalidUri);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gs://test-bucket/dir//test-object",
        "gs://test-bucket//",
        "gs://test-bucket/dir%2F%2Ftest-object"
      })
  void getItemIdFromUri_consecutiveSlashes_throwsIllegalArgumentException(
      String consecutiveSlashUri) {
    URI uri = URI.create(consecutiveSlashUri);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromUri(uri));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("GCS path must not have consecutive '/' characters: " + consecutiveSlashUri);
  }

  @Test
  void removeTrailingSlash_withTrailingSlash_removesSlash() {
    String result = UriUtil.removeTrailingSlash(TEST_PATH + "/");

    assertThat(result).isEqualTo(TEST_PATH);
  }

  @Test
  void removeTrailingSlash_withoutTrailingSlash_returnsSame() {
    String result = UriUtil.removeTrailingSlash(TEST_PATH);

    assertThat(result).isEqualTo(TEST_PATH);
  }

  @Test
  void removeTrailingSlash_nullPath_returnsNull() {
    String result = UriUtil.removeTrailingSlash(null);

    assertThat(result).isNull();
  }

  @Test
  void toDirectoryPath_withoutTrailingSlash_appendsSlash() {
    String result = UriUtil.toDirectoryPath(TEST_PATH);

    assertThat(result).isEqualTo(TEST_PATH + "/");
  }

  @Test
  void toDirectoryPath_withTrailingSlash_returnsOriginal() {
    String result = UriUtil.toDirectoryPath(TEST_PATH + "/");

    assertThat(result).isEqualTo(TEST_PATH + "/");
  }

  @Test
  void toDirectoryPath_nullPath_returnsNull() {
    String result = UriUtil.toDirectoryPath(null);

    assertThat(result).isNull();
  }

  @Test
  void toDirectoryPath_emptyPath_returnsEmpty() {
    String result = UriUtil.toDirectoryPath("");

    assertThat(result).isEmpty();
  }

  @Test
  void getStringPath_gcsObject_returnsObjectPath() {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    String result = UriUtil.getStringPath(itemId);
    assertThat(result).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
  }

  @Test
  void getStringPath_bucket_returnsBucketPath() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    String result = UriUtil.getStringPath(itemId);
    assertThat(result).isEqualTo("gs://" + TEST_BUCKET);
  }

  @Test
  void getStringPath_gcsObjectWithGeneration_omitsGeneration() {
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(1234L)
            .build();

    String result = UriUtil.getStringPath(itemId);

    assertThat(result).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
  }

  @Test
  void getStringPath_root_returnsRootPath() {
    String result = UriUtil.getStringPath(GcsItemId.ROOT);
    assertThat(result).isEqualTo("gs:/");
  }

  @Test
  void getStringPath_nullItemId_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.getStringPath(null));
    assertThat(e).hasMessageThat().isEqualTo("itemId should not be null");
  }

  @ParameterizedTest
  @MethodSource("provideObjectNames")
  void toUri_objectName_roundTripsThroughGetItemIdFromUri(String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    GcsItemId roundTripped = UriUtil.getItemIdFromUri(UriUtil.toUri(itemId));

    assertThat(roundTripped).isEqualTo(itemId);
  }

  private static Stream<String> provideObjectNames() {
    return Stream.of(
        "plain.txt",
        "a b.txt",
        "dt=2024-01-01 10%3A00%3A00/",
        "a%20b.txt",
        "a%zz.txt",
        "a%.txt",
        "a#b.txt",
        "a?b.txt",
        "a[1].txt",
        "a{1}.txt",
        "a|b.txt",
        "a^b.txt",
        "a`b.txt",
        "a\"b.txt",
        "a<b>.txt",
        "a\\b.txt",
        "café-日本語.txt",
        "a\u00a0b.txt", // A non-breaking space.
        "a\tb.txt",
        "a+b=c&d;e,f'g(h)!i*j~k:l@m.txt");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "plain.txt",
        "dir/subdir/part-0.parquet",
        "dir/",
        "café-日本語.txt",
        "a+b=c&d;e,f'g(h)!i*j~k:l@m.txt"
      })
  void toUri_objectNameOfLegalUriCharacters_returnsLiteralPath(String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    URI uri = UriUtil.toUri(itemId);

    assertThat(uri.toString()).isEqualTo("gs://" + TEST_BUCKET + "/" + objectName);
  }

  @ParameterizedTest
  @MethodSource("provideObjectNamesAndEncodedPaths")
  void toUri_objectNameWithIllegalUriCharacters_percentEncodes(
      String objectName, String encodedPath) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    URI uri = UriUtil.toUri(itemId);

    assertThat(uri.toString()).isEqualTo("gs://" + TEST_BUCKET + "/" + encodedPath);
  }

  private static Stream<Arguments> provideObjectNamesAndEncodedPaths() {
    return Stream.of(
        Arguments.of("dir/a b.txt", "dir/a%20b.txt"),
        Arguments.of("a%20b.txt", "a%2520b.txt"),
        Arguments.of("a#b?c.txt", "a%23b%3Fc.txt"),
        Arguments.of("dt=2024-01-01 10%3A00%3A00/", "dt=2024-01-01%2010%253A00%253A00/"),
        Arguments.of("a[1]{2}|^`\"<>\\.txt", "a%5B1%5D%7B2%7D%7C%5E%60%22%3C%3E%5C.txt"),
        Arguments.of("a\u00a0b\tc.txt", "a%C2%A0b%09c.txt"));
  }

  @Test
  void toUri_bucket_returnsUriWithTrailingSlash() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    URI uri = UriUtil.toUri(itemId);

    assertThat(uri.toString()).isEqualTo("gs://" + TEST_BUCKET + "/");
  }

  @Test
  void toUri_bucket_roundTripsThroughGetItemIdFromUri() {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    GcsItemId roundTripped = UriUtil.getItemIdFromUri(UriUtil.toUri(itemId));

    assertThat(roundTripped).isEqualTo(itemId);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "/a.txt", "dir//a.txt", "dir//"})
  void toUri_objectNameWithLeadingOrConsecutiveSlashes_returnsUriThatGetItemIdFromUriRejects(
      String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    URI uri = UriUtil.toUri(itemId);

    assertThat(uri.toString()).isEqualTo("gs://" + TEST_BUCKET + "/" + objectName);
    assertThrows(IllegalArgumentException.class, () -> UriUtil.getItemIdFromUri(uri));
  }

  @Test
  void toUri_everyCharacterInObjectName_equalsUriOfQuotingConstructor() throws URISyntaxException {
    // toUri parses the text directly when it sees nothing to quote. This checks that shortcut
    // against the quoting constructor for every UTF-16 code unit, alone and inside a name.
    List<String> mismatches = new ArrayList<>();

    for (int codeUnit = Character.MIN_VALUE; codeUnit <= Character.MAX_VALUE; codeUnit++) {
      char c = (char) codeUnit;
      for (String objectName : new String[] {String.valueOf(c), "dir/a" + c + "b.txt"}) {
        GcsItemId itemId =
            GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
        URI expected = new URI("gs", TEST_BUCKET, "/" + objectName, null, null);
        URI actual = UriUtil.toUri(itemId);
        if (!actual.equals(expected) || !actual.toString().equals(expected.toString())) {
          mismatches.add(String.format("U+%04X: %s but expected %s", codeUnit, actual, expected));
        }
      }
    }

    assertThat(mismatches).isEmpty();
  }

  @Test
  void toUri_everyAsciiCharacterInBucketName_matchesQuotingConstructor() {
    List<String> bucketNames = new ArrayList<>();
    bucketNames.add("");
    for (char c = 0; c < 128; c++) {
      bucketNames.add(String.valueOf(c));
      bucketNames.add("a" + c + "b");
    }
    List<String> mismatches = new ArrayList<>();

    for (String bucketName : bucketNames) {
      GcsItemId itemId =
          GcsItemId.builder().setBucketName(bucketName).setObjectName(TEST_OBJECT).build();
      String expected;
      try {
        expected = new URI("gs", bucketName, "/" + TEST_OBJECT, null, null).toString();
      } catch (URISyntaxException e) {
        expected = "IllegalArgumentException";
      }
      String actual;
      try {
        actual = UriUtil.toUri(itemId).toString();
      } catch (IllegalArgumentException e) {
        actual = "IllegalArgumentException";
      }
      if (!actual.equals(expected)) {
        mismatches.add(
            String.format("bucket '%s': %s but expected %s", bucketName, actual, expected));
      }
    }

    assertThat(mismatches).isEmpty();
  }

  @Test
  void toUri_root_returnsRootUri() {
    URI uri = UriUtil.toUri(GcsItemId.ROOT);

    assertThat(uri.toString()).isEqualTo("gs:/");
  }

  @Test
  void toUri_gcsObjectWithGeneration_omitsGeneration() {
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(1234L)
            .build();

    URI uri = UriUtil.toUri(itemId);

    assertThat(uri.toString()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_OBJECT);
  }

  @Test
  void toUri_bucketNameNotLegalInUri_throwsIllegalArgumentException() {
    GcsItemId itemId = GcsItemId.builder().setBucketName("a[b").setObjectName(TEST_OBJECT).build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.toUri(itemId));

    assertThat(e).hasMessageThat().isEqualTo("Cannot create a URI for gs://a[b/" + TEST_OBJECT);
    assertThat(e).hasCauseThat().isInstanceOf(URISyntaxException.class);
  }

  @Test
  void toUri_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> UriUtil.toUri(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {TEST_OBJECT, TEST_OBJECT + "/", TEST_PATH, TEST_PATH + "/"})
  void validateDirectoryItemId_validObjectName_doesNotThrow(String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    assertDoesNotThrow(() -> UriUtil.validateDirectoryItemId(itemId));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "//", "/dir/subdir", "dir//subdir", "dir/subdir//"})
  void validateDirectoryItemId_leadingOrConsecutiveSlashes_throwsIllegalArgumentException(
      String objectName) {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(itemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Object name cannot start with '/' or contain consecutive '/': " + itemId);
  }

  @Test
  void validateDirectoryItemId_nonObjectItemId_throwsIllegalArgumentException() {
    GcsItemId bucketItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId emptyBucketItemId =
        GcsItemId.builder().setBucketName("").setObjectName(TEST_PATH).build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(bucketItemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo("Expected a directory itemId within a bucket but got: " + bucketItemId);
    assertThrows(
        IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(GcsItemId.ROOT));
    assertThrows(
        IllegalArgumentException.class, () -> UriUtil.validateDirectoryItemId(emptyBucketItemId));
  }

  @Test
  void validateDirectoryItemId_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> UriUtil.validateDirectoryItemId(null));
  }

  @Test
  void withoutContentGeneration_stripsGenerationAndPreservesOtherFields() {
    GcsItemId withGen =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_OBJECT)
            .setContentGeneration(42L)
            .build();
    GcsItemId withoutGen =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();

    assertThat(UriUtil.withoutContentGeneration(withGen)).isEqualTo(withoutGen);
    assertThat(UriUtil.withoutContentGeneration(withoutGen)).isSameInstanceAs(withoutGen);
    assertThrows(NullPointerException.class, () -> UriUtil.withoutContentGeneration(null));
  }

  @Test
  void getParentId_variousItemTypes_returnsExpectedParentWithoutGeneration() {
    GcsItemId bucket = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId topFile =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("file.txt")
            .setContentGeneration(10L)
            .build();
    GcsItemId topDir = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    GcsItemId leadingSlashFile =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("/file.txt").build();
    GcsItemId leadingSlashDir =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("/dir/").build();
    GcsItemId nestedFile =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("a/b/file.txt")
            .setContentGeneration(11L)
            .build();
    GcsItemId nestedDir =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a/b/c/").build();
    GcsItemId expectedNestedParent =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a/b/").build();

    assertThat(UriUtil.getParentId(GcsItemId.ROOT)).isEmpty();
    assertThat(UriUtil.getParentId(bucket)).hasValue(GcsItemId.ROOT);
    assertThat(UriUtil.getParentId(topFile)).hasValue(bucket);
    assertThat(UriUtil.getParentId(topDir)).hasValue(bucket);
    assertThat(UriUtil.getParentId(leadingSlashFile)).hasValue(bucket);
    assertThat(UriUtil.getParentId(leadingSlashDir)).hasValue(bucket);
    assertThat(UriUtil.getParentId(nestedFile)).hasValue(expectedNestedParent);
    assertThat(UriUtil.getParentId(nestedDir)).hasValue(expectedNestedParent);
    assertThrows(NullPointerException.class, () -> UriUtil.getParentId(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {".", "..", "./", "../", "a/./b", "a/../b", "a/.", "a/..", "/.", "/.."})
  void hasDotSegment_pathsWithDotSegments_returnsTrue(String path) {
    assertThat(UriUtil.hasDotSegment(path)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", "/", "a/b/c", "a/.hidden", "a/..hidden", "a/...", "a/foo.", "a/foo.."})
  void hasDotSegment_pathsWithoutDotSegments_returnsFalse(String path) {
    assertThat(UriUtil.hasDotSegment(path)).isFalse();
  }

  @Test
  void hasDotSegment_nullPath_returnsFalse() {
    assertThat(UriUtil.hasDotSegment(null)).isFalse();
  }

  @Test
  void getLeafName_variousItemTypes_returnsExpectedLeafWithoutTrailingSlash() {
    GcsItemId bucket = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId topFile =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("file.txt").build();
    GcsItemId nestedFile =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a/b/file.txt").build();
    GcsItemId topDir = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    GcsItemId nestedDir =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a/b/dir/").build();
    GcsItemId slashOnly = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("/").build();

    assertThat(UriUtil.getLeafName(GcsItemId.ROOT)).isEmpty();
    assertThat(UriUtil.getLeafName(bucket)).hasValue(TEST_BUCKET);
    assertThat(UriUtil.getLeafName(topFile)).hasValue("file.txt");
    assertThat(UriUtil.getLeafName(nestedFile)).hasValue("file.txt");
    assertThat(UriUtil.getLeafName(topDir)).hasValue("dir");
    assertThat(UriUtil.getLeafName(nestedDir)).hasValue("dir");
    assertThat(UriUtil.getLeafName(slashOnly)).isEmpty();
    assertThrows(NullPointerException.class, () -> UriUtil.getLeafName(null));
  }

  @Test
  void isSubdirectory_sameBucketAndBoundaryCases_returnsExpectedResult() {
    GcsItemId bucket = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId otherBucket = GcsItemId.builder().setBucketName("other-bucket").build();
    GcsItemId dirA = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a/").build();
    GcsItemId dirAB = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("ab/").build();
    GcsItemId child =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("a/sub/file").build();
    GcsItemId otherBucketChild =
        GcsItemId.builder().setBucketName("other-bucket").setObjectName("a/sub/").build();

    assertThat(UriUtil.isSubdirectory(dirA, child)).isTrue();
    assertThat(UriUtil.isSubdirectory(bucket, dirA)).isTrue();
    assertThat(UriUtil.isSubdirectory(dirA, dirA)).isFalse();
    assertThat(UriUtil.isSubdirectory(dirA, dirAB)).isFalse();
    assertThat(UriUtil.isSubdirectory(dirA, bucket)).isFalse();
    assertThat(UriUtil.isSubdirectory(bucket, bucket)).isFalse();
    assertThat(UriUtil.isSubdirectory(dirA, otherBucketChild)).isFalse();
    assertThat(UriUtil.isSubdirectory(GcsItemId.ROOT, bucket)).isFalse();
    assertThat(UriUtil.isSubdirectory(otherBucket, GcsItemId.ROOT)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "/child", ".", "..", "sub/../escape", "sub/./file", "a//b", "a//"})
  void validateRelativeChildPath_invalidPaths_throwsIOException(String invalidRelativePath) {
    GcsItemId src =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("src/file").build();
    GcsItemId dstDir = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dst/").build();

    IOException e =
        assertThrows(
            IOException.class,
            () -> UriUtil.validateRelativeChildPath(invalidRelativePath, src, dstDir));

    assertThat(e).hasMessageThat().contains("invalid relative path");
  }

  @Test
  void resolveChildId_validFileAndDirectoryUnderBucketAndSubdir_resolvesCleanly()
      throws IOException {
    GcsItemId src =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("src/item").build();
    GcsItemId bucket = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId subdir = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dst/").build();

    assertThat(UriUtil.resolveChildId(bucket, "item", src, false))
        .isEqualTo(GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("item").build());
    assertThat(UriUtil.resolveChildId(bucket, "item", src, true))
        .isEqualTo(GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("item/").build());
    assertThat(UriUtil.resolveChildId(subdir, "item", src, false))
        .isEqualTo(
            GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dst/item").build());
    assertThat(UriUtil.resolveChildId(subdir, "item", src, true))
        .isEqualTo(
            GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dst/item/").build());
  }
}
