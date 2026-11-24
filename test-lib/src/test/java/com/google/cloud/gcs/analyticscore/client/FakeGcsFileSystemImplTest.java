/*
 * Copyright 2025 Google LLC
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FakeGcsFileSystemImplTest {

  @Test
  void constructor_initializedWithFakeGcsClient() {
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();

    FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(options);

    assertNotNull(fakeFileSystem);
    assertTrue(fakeFileSystem.getGcsClient() instanceof FakeGcsClientImpl);
  }

  @Test
  void listFileInfo_directory_returnsImmediateChildrenSortedByUri() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/");
    putBlob(bucket, "dir/file.txt");
    putBlob(bucket, "dir/implicit/nested.txt");
    putBlob(bucket, "dir/sub/");
    putBlob(bucket, "dir/sub/nested.txt");
    putBlob(bucket, "other/file.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir/"));

      assertThat(describeListed(listed))
          .containsExactly(
              "gs://" + bucket + "/dir/sub/ PLACEHOLDER_DIRECTORY",
              "gs://" + bucket + "/dir/file.txt OBJECT",
              "gs://" + bucket + "/dir/implicit/ INFERRED_DIRECTORY")
          .inOrder();
    }
  }

  @Test
  void listFileInfo_objectPath_returnsOnlyThatObject() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/file.txt");
    putBlob(bucket, "dir/other.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir/file.txt"));

      assertThat(describeListed(listed)).containsExactly("gs://" + bucket + "/dir/file.txt OBJECT");
    }
  }

  @Test
  void listFileInfo_directoryPathWithoutTrailingSlash_returnsChildren() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/file.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed = fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir"));

      assertThat(describeListed(listed)).containsExactly("gs://" + bucket + "/dir/file.txt OBJECT");
    }
  }

  @Test
  void listFileInfo_emptyDirectoryWithPlaceholder_returnsEmptyList() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "empty/");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/empty/"));

      assertThat(listed).isEmpty();
    }
  }

  @Test
  void listFileInfo_missingDirectory_throwsFileNotFoundException() {
    String bucket = newBucketName();
    putBlob(bucket, "dir/file.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      assertThrows(
          FileNotFoundException.class,
          () -> fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/missing/")));
    }
  }

  @Test
  void listFileInfo_childNameWithSpace_returnsUriThatGetFileInfoResolves() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "dir/my report.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      List<GcsFileInfo> listed =
          fakeFileSystem.listFileInfo(URI.create("gs://" + bucket + "/dir/"));
      GcsFileInfo fileInfo = fakeFileSystem.getFileInfo(listed.get(0).getUri());

      assertThat(describeListed(listed))
          .containsExactly("gs://" + bucket + "/dir/my%20report.txt OBJECT");
      assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue("dir/my report.txt");
    }
  }

  @Test
  void rename_flatFileWithPercentEncodedSpecialChars_movesObject() throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "src file?1#2%3.txt");
    GcsItemId srcId =
        GcsItemId.builder().setBucketName(bucket).setObjectName("src file?1#2%3.txt").build();
    GcsItemId dstId =
        GcsItemId.builder().setBucketName(bucket).setObjectName("dst file?1#2%3.txt").build();

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      fakeFileSystem.rename(srcId.toUri(), dstId.toUri());

      assertThrows(FileNotFoundException.class, () -> fakeFileSystem.getFileInfo(srcId));
      assertThat(fakeFileSystem.getFileInfo(dstId).getItemInfo().getItemId().getObjectName())
          .hasValue("dst file?1#2%3.txt");
    }
  }

  @Test
  void rename_flatFileIntoExistingDirectoryAndConflict_movesThenRejectsDuplicate()
      throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "a.txt");
    putBlob(bucket, "b.txt");
    putBlob(bucket, "target/");
    putBlob(bucket, "target/b.txt");

    try (FakeGcsFileSystemImpl fakeFileSystem = newFakeFileSystem()) {
      fakeFileSystem.rename(
          URI.create("gs://" + bucket + "/a.txt"), URI.create("gs://" + bucket + "/target/"));
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "a.txt"))).isNull();
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "target/a.txt"))).isNotNull();

      assertThrows(
          FileAlreadyExistsException.class,
          () ->
              fakeFileSystem.rename(
                  URI.create("gs://" + bucket + "/b.txt"),
                  URI.create("gs://" + bucket + "/target/")));
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "b.txt"))).isNotNull();
    }
  }

  @Test
  void rename_flatDirectoryWithMarkerAndImplicitParent_movesAllAndRepairsParent()
      throws IOException {
    String bucket = newBucketName();
    putBlob(bucket, "parent/src/part-0.parquet");
    putBlob(bucket, "parent/src/sub/part-1.parquet");
    putBlob(bucket, "parent/src/_SUCCESS");
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setMarkerFilePattern("_SUCCESS")
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(4).build())
            .build();

    try (FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(options)) {
      fakeFileSystem.rename(
          URI.create("gs://" + bucket + "/parent/src/"), URI.create("gs://" + bucket + "/dst/"));

      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "parent/src/part-0.parquet")))
          .isNull();
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "dst/part-0.parquet")))
          .isNotNull();
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "dst/sub/part-1.parquet")))
          .isNotNull();
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "dst/_SUCCESS"))).isNotNull();
      Blob repairedParent = FakeGcsClientImpl.storage.get(BlobId.of(bucket, "parent/"));
      assertThat(repairedParent).isNotNull();
      assertThat(repairedParent.getSize()).isEqualTo(0L);
    }
  }

  @Test
  void rename_hnsFolderAndIntoExistingFolder_renamesAtomicallyViaFakeStorageControl()
      throws IOException {
    String bucket = newBucketName();
    GcsFileSystemOptions options = GcsFileSystemOptions.builder().build();
    ExecutorService readExecutor = Executors.newCachedThreadPool();
    Telemetry telemetry = new Telemetry(ImmutableList.of());
    AnalyticsCacheManager cacheManager = new AnalyticsCacheManager(options.getGcsCacheOptions());
    FakeGcsClientImpl hnsClient =
        new FakeGcsClientImpl(
            options.getGcsClientOptions(),
            () -> readExecutor,
            telemetry,
            cacheManager.getBucketPropertiesCache()) {
          @Override
          public BucketProperties getBucketProperties(String bucketName) {
            return BucketProperties.create(true, false);
          }
        };
    try (GcsFileSystemImpl hnsFs =
        new GcsFileSystemImpl(hnsClient, options, telemetry, cacheManager)) {
      hnsFs.mkdirs(URI.create("gs://" + bucket + "/src/nested/"));
      hnsFs.mkdirs(URI.create("gs://" + bucket + "/target/"));
      putBlob(bucket, "src/nested/data.txt");

      hnsFs.rename(
          URI.create("gs://" + bucket + "/src/"), URI.create("gs://" + bucket + "/target/"));

      assertThrows(
          FileNotFoundException.class,
          () -> hnsFs.getFileInfo(URI.create("gs://" + bucket + "/src/")));
      assertThat(
              hnsFs
                  .getFileInfo(URI.create("gs://" + bucket + "/target/src/"))
                  .getItemInfo()
                  .getItemType())
          .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
      assertThat(FakeGcsClientImpl.storage.get(BlobId.of(bucket, "target/src/nested/data.txt")))
          .isNotNull();

      // Leading/trailing space HNS folder round-trip via percent-encoded URI.
      GcsItemId spaceSrc = GcsItemId.builder().setBucketName(bucket).setObjectName(" dir/").build();
      GcsItemId spaceDst = GcsItemId.builder().setBucketName(bucket).setObjectName("dir /").build();
      hnsFs.mkdirs(spaceSrc);
      hnsFs.rename(spaceSrc.toUri(), spaceDst.toUri());

      assertThrows(FileNotFoundException.class, () -> hnsFs.getFileInfo(spaceSrc));
      assertThat(hnsFs.getFileInfo(spaceDst).getItemInfo().getItemType())
          .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    } finally {
      readExecutor.shutdownNow();
    }
  }

  private static FakeGcsFileSystemImpl newFakeFileSystem() {
    return new FakeGcsFileSystemImpl(GcsFileSystemOptions.builder().build());
  }

  private static String newBucketName() {
    return "fake-fs-list-" + UUID.randomUUID();
  }

  private static void putBlob(String bucket, String name) {
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(BlobId.of(bucket, name)).build(),
        name.endsWith("/") ? new byte[0] : "data".getBytes(StandardCharsets.UTF_8));
  }

  /** Returns each listed item as its URI followed by its item type. */
  private static List<String> describeListed(List<GcsFileInfo> listed) {
    return listed.stream()
        .map(fileInfo -> fileInfo.getUri() + " " + fileInfo.getItemInfo().getItemType())
        .collect(Collectors.toList());
  }
}
