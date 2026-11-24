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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.CharMatcher;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.ListFoldersRequest;
import com.google.storage.control.v2.StorageControlClient;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link GcsFileSystem#rename} on live flat and HNS buckets. */
class GcsFileSystemRenameIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(GcsFileSystemRenameIntegrationTest.class);
  private static final String FLAT_BUCKET_PROPERTY = "gcs.integration.test.bucket";
  private static final String HNS_BUCKET_PROPERTY = "gcs.integration.test.hns.bucket";
  private static final byte[] CONTENT_A = "alpha-content".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CONTENT_B = "beta-content".getBytes(StandardCharsets.UTF_8);

  private static Storage storage;
  private String runPrefix;
  private final List<String> flatPrefixesToClean = new ArrayList<>();
  private final List<String> hnsPrefixesToClean = new ArrayList<>();

  @BeforeAll
  static void setUpClass() {
    storage = StorageOptions.getDefaultInstance().getService();
  }

  @AfterAll
  static void tearDownClass() throws Exception {
    if (storage != null) {
      storage.close();
      storage = null;
    }
  }

  @BeforeEach
  void setUp() {
    runPrefix = "test-fs-rename-" + UUID.randomUUID() + "/";
  }

  @AfterEach
  void tearDown() {
    String flatBucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    if (flatBucket != null && !flatBucket.isEmpty()) {
      for (String prefix : flatPrefixesToClean) {
        deleteAllBlobsWithPrefix(flatBucket, prefix);
      }
    }
    String hnsBucket = System.getProperty(HNS_BUCKET_PROPERTY);
    if (hnsBucket != null && !hnsBucket.isEmpty()) {
      for (String prefix : hnsPrefixesToClean) {
        deleteAllBlobsWithPrefix(hnsBucket, prefix);
        deleteAllFoldersWithPrefix(hnsBucket, prefix);
      }
    }
    flatPrefixesToClean.clear();
    hnsPrefixesToClean.clear();
  }

  private void deleteAllBlobsWithPrefix(String bucket, String prefix) {
    try {
      for (Blob blob : storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
        try {
          storage.delete(blob.getBlobId());
        } catch (Exception e) {
          LOG.warn("Failed to delete blob {} during cleanup", blob.getBlobId(), e);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to list blobs under gs://{}/{} during cleanup", bucket, prefix, e);
    }
  }

  private void deleteAllFoldersWithPrefix(String bucket, String prefix) {
    try (StorageControlClient controlClient = StorageControlClient.create()) {
      List<String> folderNames = new ArrayList<>();
      for (Folder folder :
          controlClient
              .listFolders(
                  ListFoldersRequest.newBuilder()
                      .setParent("projects/_/buckets/" + bucket)
                      .setPrefix(prefix)
                      .build())
              .iterateAll()) {
        folderNames.add(folder.getName());
      }
      folderNames.sort(
          Comparator.comparingInt(
                  (String name) -> CharMatcher.is('/').countIn(UriUtil.removeTrailingSlash(name)))
              .thenComparingInt(String::length)
              .reversed());
      for (String folderName : folderNames) {
        try {
          controlClient.deleteFolder(
              DeleteFolderRequest.newBuilder().setName(folderName).build());
        } catch (Exception e) {
          LOG.warn("Failed to delete folder {} during cleanup", folderName, e);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to clean HNS folders under gs://{}/{}", bucket, prefix, e);
    }
  }

  private void writeBlob(String bucket, String objectName, byte[] content) {
    storage.create(BlobInfo.newBuilder(BlobId.of(bucket, objectName)).build(), content);
  }

  // --- Flat bucket integration tests ---

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void rename_flatFileAndIntoExistingDir_movesObjects() throws IOException {
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    flatPrefixesToClean.add(runPrefix);
    String src = runPrefix + "src.txt";
    String renamed = runPrefix + "renamed.txt";
    String targetDir = runPrefix + "target-dir/";
    String movedInDir = targetDir + "renamed.txt";
    writeBlob(bucket, src, CONTENT_A);
    writeBlob(bucket, targetDir, new byte[0]);

    try (GcsFileSystem fs = new GcsFileSystemImpl(GcsFileSystemOptions.builder().build())) {
      fs.rename(
          URI.create("gs://" + bucket + "/" + src),
          URI.create("gs://" + bucket + "/" + renamed));
      assertThat(storage.get(BlobId.of(bucket, src))).isNull();
      assertThat(storage.readAllBytes(BlobId.of(bucket, renamed))).isEqualTo(CONTENT_A);

      fs.rename(
          URI.create("gs://" + bucket + "/" + renamed),
          URI.create("gs://" + bucket + "/" + targetDir));
      assertThat(storage.get(BlobId.of(bucket, renamed))).isNull();
      assertThat(storage.readAllBytes(BlobId.of(bucket, movedInDir))).isEqualTo(CONTENT_A);
    }
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void rename_flatNestedDirectoryWithMarkerFileAndEmptyDir_movesAllObjects() throws IOException {
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    flatPrefixesToClean.add(runPrefix);
    String srcDir = runPrefix + "src-dir/";
    String dstDir = runPrefix + "dst-dir/";
    String emptySrc = runPrefix + "empty-src/";
    String emptyDst = runPrefix + "empty-dst/";

    writeBlob(bucket, srcDir, new byte[0]);
    writeBlob(bucket, srcDir + "part-0.txt", CONTENT_A);
    writeBlob(bucket, srcDir + "sub/part-1.txt", CONTENT_B);
    writeBlob(bucket, srcDir + "_SUCCESS", new byte[0]);
    writeBlob(bucket, emptySrc, new byte[0]);

    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder().setMarkerFilePattern("_SUCCESS").build();
    try (GcsFileSystem fs = new GcsFileSystemImpl(options)) {
      fs.rename(
          URI.create("gs://" + bucket + "/" + srcDir),
          URI.create("gs://" + bucket + "/" + dstDir));

      assertThat(storage.get(BlobId.of(bucket, srcDir))).isNull();
      assertThat(storage.get(BlobId.of(bucket, srcDir + "part-0.txt"))).isNull();
      assertThat(storage.get(BlobId.of(bucket, srcDir + "sub/part-1.txt"))).isNull();
      assertThat(storage.get(BlobId.of(bucket, srcDir + "_SUCCESS"))).isNull();
      assertThat(storage.get(BlobId.of(bucket, dstDir))).isNotNull();
      assertThat(storage.readAllBytes(BlobId.of(bucket, dstDir + "part-0.txt")))
          .isEqualTo(CONTENT_A);
      assertThat(storage.readAllBytes(BlobId.of(bucket, dstDir + "sub/part-1.txt")))
          .isEqualTo(CONTENT_B);
      assertThat(storage.get(BlobId.of(bucket, dstDir + "_SUCCESS"))).isNotNull();

      // Empty flat directory (only placeholder object)
      fs.rename(
          URI.create("gs://" + bucket + "/" + emptySrc),
          URI.create("gs://" + bucket + "/" + emptyDst));
      assertThat(storage.get(BlobId.of(bucket, emptySrc))).isNull();
      assertThat(storage.get(BlobId.of(bucket, emptyDst))).isNotNull();
    }
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void rename_flatImplicitParentRepair_createsZeroByteParentMarker() throws IOException {
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    flatPrefixesToClean.add(runPrefix);
    String implicitParent = runPrefix + "implicit-parent/";
    String src = implicitParent + "only-file.txt";
    String dst = runPrefix + "moved-out.txt";
    // Create only the child file (no explicit placeholder for implicitParent).
    writeBlob(bucket, src, CONTENT_A);
    assertThat(storage.get(BlobId.of(bucket, implicitParent))).isNull();

    try (GcsFileSystem fs = new GcsFileSystemImpl(GcsFileSystemOptions.builder().build())) {
      fs.rename(URI.create("gs://" + bucket + "/" + src), URI.create("gs://" + bucket + "/" + dst));

      assertThat(storage.get(BlobId.of(bucket, src))).isNull();
      assertThat(storage.readAllBytes(BlobId.of(bucket, dst))).isEqualTo(CONTENT_A);
      Blob repairedParent = storage.get(BlobId.of(bucket, implicitParent));
      assertThat(repairedParent).isNotNull();
      assertThat(repairedParent.getSize()).isEqualTo(0L);
    }
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void rename_flatConflictAndMissingErrors_throwsExpectedExceptions() throws IOException {
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    flatPrefixesToClean.add(runPrefix);
    String src = runPrefix + "src.txt";
    String existingDst = runPrefix + "existing.txt";
    writeBlob(bucket, src, CONTENT_A);
    writeBlob(bucket, existingDst, CONTENT_B);

    try (GcsFileSystem fs = new GcsFileSystemImpl(GcsFileSystemOptions.builder().build())) {
      assertThrows(
          FileAlreadyExistsException.class,
          () ->
              fs.rename(
                  URI.create("gs://" + bucket + "/" + src),
                  URI.create("gs://" + bucket + "/" + existingDst)));
      assertThrows(
          FileNotFoundException.class,
          () ->
              fs.rename(
                  URI.create("gs://" + bucket + "/" + runPrefix + "does-not-exist.txt"),
                  URI.create("gs://" + bucket + "/" + runPrefix + "dst.txt")));
      assertThrows(
          FileNotFoundException.class,
          () ->
              fs.rename(
                  URI.create("gs://" + bucket + "/" + src),
                  URI.create("gs://" + bucket + "/" + runPrefix + "missing-parent/dst.txt")));
    }
  }

  // --- HNS bucket integration tests ---

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void rename_hnsFileAndNativeFolder_renamesAtomically() throws IOException {
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    hnsPrefixesToClean.add(runPrefix);
    String srcFile = runPrefix + "file.txt";
    String renamedFile = runPrefix + "file-renamed.txt";
    String targetFolder = runPrefix + "target-folder/";
    String srcFolder = runPrefix + "src-folder/";
    String dstFolder = runPrefix + "dst-folder/";
    String emptySrcFolder = runPrefix + "empty-folder/";
    String emptyDstFolder = runPrefix + "empty-folder-renamed/";

    try (GcsFileSystem fs = new GcsFileSystemImpl(GcsFileSystemOptions.builder().build())) {
      fs.mkdirs(URI.create("gs://" + bucket + "/" + targetFolder));
      fs.mkdirs(URI.create("gs://" + bucket + "/" + srcFolder + "nested/"));
      fs.mkdirs(URI.create("gs://" + bucket + "/" + emptySrcFolder));
      writeBlob(bucket, srcFile, CONTENT_A);
      writeBlob(bucket, srcFolder + "nested/data.txt", CONTENT_B);

      // Single file rename & move into existing native folder
      fs.rename(
          URI.create("gs://" + bucket + "/" + srcFile),
          URI.create("gs://" + bucket + "/" + renamedFile));
      assertThat(storage.get(BlobId.of(bucket, srcFile))).isNull();
      assertThat(storage.readAllBytes(BlobId.of(bucket, renamedFile))).isEqualTo(CONTENT_A);

      fs.rename(
          URI.create("gs://" + bucket + "/" + renamedFile),
          URI.create("gs://" + bucket + "/" + targetFolder));
      assertThat(storage.get(BlobId.of(bucket, renamedFile))).isNull();
      assertThat(storage.readAllBytes(BlobId.of(bucket, targetFolder + "file-renamed.txt")))
          .isEqualTo(CONTENT_A);

      // Nested native folder rename
      fs.rename(
          URI.create("gs://" + bucket + "/" + srcFolder),
          URI.create("gs://" + bucket + "/" + dstFolder));
      assertThrows(
          FileNotFoundException.class,
          () -> fs.getFileInfo(URI.create("gs://" + bucket + "/" + srcFolder)));
      assertThat(
              fs.getFileInfo(URI.create("gs://" + bucket + "/" + dstFolder))
                  .getItemInfo()
                  .getItemType())
          .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
      assertThat(storage.readAllBytes(BlobId.of(bucket, dstFolder + "nested/data.txt")))
          .isEqualTo(CONTENT_B);

      // Empty native folder rename
      fs.rename(
          URI.create("gs://" + bucket + "/" + emptySrcFolder),
          URI.create("gs://" + bucket + "/" + emptyDstFolder));
      assertThrows(
          FileNotFoundException.class,
          () -> fs.getFileInfo(URI.create("gs://" + bucket + "/" + emptySrcFolder)));
      assertThat(
              fs.getFileInfo(URI.create("gs://" + bucket + "/" + emptyDstFolder))
                  .getItemInfo()
                  .getItemType())
          .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
    }
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void rename_hnsConflictAndMissingErrors_throwsExpectedExceptions() throws IOException {
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    hnsPrefixesToClean.add(runPrefix);
    String src = runPrefix + "src.txt";
    String existingDst = runPrefix + "existing.txt";
    writeBlob(bucket, src, CONTENT_A);
    writeBlob(bucket, existingDst, CONTENT_B);

    try (GcsFileSystem fs = new GcsFileSystemImpl(GcsFileSystemOptions.builder().build())) {
      assertThrows(
          FileAlreadyExistsException.class,
          () ->
              fs.rename(
                  URI.create("gs://" + bucket + "/" + src),
                  URI.create("gs://" + bucket + "/" + existingDst)));
      assertThrows(
          FileNotFoundException.class,
          () ->
              fs.rename(
                  URI.create("gs://" + bucket + "/" + runPrefix + "missing.txt"),
                  URI.create("gs://" + bucket + "/" + runPrefix + "dst.txt")));
      assertThrows(
          FileNotFoundException.class,
          () ->
              fs.rename(
                  URI.create("gs://" + bucket + "/" + src),
                  URI.create("gs://" + bucket + "/" + runPrefix + "missing-parent/dst.txt")));
    }
  }
}
