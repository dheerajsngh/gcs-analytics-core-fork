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
import java.nio.file.DirectoryNotEmptyException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GcsFileSystemDeleteIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(GcsFileSystemDeleteIntegrationTest.class);

  private static final String FLAT_BUCKET_PROPERTY = "gcs.integration.test.bucket";
  private static final String HNS_BUCKET_PROPERTY = "gcs.integration.test.hns.bucket";
  private static final String PROJECT_ID_PROPERTY = "gcs.integration.test.project-id";
  private static final byte[] TEST_CONTENT = "delete-it-payload".getBytes(StandardCharsets.UTF_8);

  private Storage storage;
  private GcsFileSystemImpl fileSystem;
  private List<BlobId> blobsToCleanup;
  private List<String> foldersToCleanup;
  private List<String> bucketsToCleanup;
  private List<String> hnsBucketsToCleanup;

  @BeforeEach
  void setUp() {
    storage = StorageOptions.getDefaultInstance().getService();
    blobsToCleanup = new ArrayList<>();
    foldersToCleanup = new ArrayList<>();
    bucketsToCleanup = new ArrayList<>();
    hnsBucketsToCleanup = new ArrayList<>();
    fileSystem = createFileSystem(GcsFileSystemOptions.builder());
  }

  @AfterEach
  void tearDown() {
    try {
      if (fileSystem != null) {
        try {
          fileSystem.close();
        } catch (Exception e) {
          LOG.warn("Failed to close fileSystem during cleanup", e);
        }
      }
      if (storage != null) {
        for (BlobId blobId : blobsToCleanup) {
          try {
            storage.delete(blobId);
          } catch (Exception e) {
            LOG.warn("Failed to delete blob {} during cleanup", blobId, e);
          }
        }
      }
      deleteFoldersDeepestFirst(foldersToCleanup);
      for (String bucketName : hnsBucketsToCleanup) {
        cleanupEphemeralBucket(bucketName, /* isHns= */ true);
      }
      for (String bucketName : bucketsToCleanup) {
        cleanupEphemeralBucket(bucketName, /* isHns= */ false);
      }
    } finally {
      blobsToCleanup.clear();
      foldersToCleanup.clear();
      bucketsToCleanup.clear();
      hnsBucketsToCleanup.clear();
    }
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatSingleFile_deletesObject() throws IOException {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    String objectName = "it-delete-file-" + UUID.randomUUID() + ".txt";
    createBlob(bucket, objectName, TEST_CONTENT);

    // Act
    fileSystem.delete(URI.create("gs://" + bucket + "/" + objectName), /* recursive= */ false);

    // Assert
    assertThat(storage.get(BlobId.of(bucket, objectName))).isNull();
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatMissingPath_throwsFileNotFoundException() {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    URI missingUri = URI.create("gs://" + bucket + "/it-delete-missing-" + UUID.randomUUID());

    // Act & Assert
    assertThrows(FileNotFoundException.class, () -> fileSystem.delete(missingUri, false));
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatSingleFileInImplicitParent_repairsParentDirectory() throws IOException {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    String parentDir = "it-delete-repair-" + UUID.randomUUID() + "/";
    String objectName = parentDir + "only-file.txt";
    blobsToCleanup.add(BlobId.of(bucket, parentDir));
    createBlob(bucket, objectName, TEST_CONTENT);

    // Act
    fileSystem.delete(URI.create("gs://" + bucket + "/" + objectName), /* recursive= */ false);

    // Assert
    assertThat(storage.get(BlobId.of(bucket, objectName))).isNull();
    GcsFileInfo parentInfo = fileSystem.getFileInfo(URI.create("gs://" + bucket + "/" + parentDir));
    assertThat(parentInfo.getItemInfo().getItemType())
        .isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    assertThat(parentInfo.getItemInfo().getSize()).isEqualTo(0L);
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatSingleFileWithAutoRepairDisabled_doesNotCreateParentPlaceholder()
      throws IOException {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    String parentDir = "it-delete-norepair-" + UUID.randomUUID() + "/";
    String objectName = parentDir + "only-file.txt";
    createBlob(bucket, objectName, TEST_CONTENT);

    try (GcsFileSystemImpl noRepairFs =
        createFileSystem(
            GcsFileSystemOptions.builder().setAutoRepairImplicitDirectoriesEnabled(false))) {
      // Act
      noRepairFs.delete(URI.create("gs://" + bucket + "/" + objectName), /* recursive= */ false);

      // Assert
      assertThat(storage.get(BlobId.of(bucket, objectName))).isNull();
      assertThrows(
          FileNotFoundException.class,
          () -> noRepairFs.getFileInfo(URI.create("gs://" + bucket + "/" + parentDir)));
    }
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatNonRecursiveDirectory_throwsWhenNonEmptyAndDeletesWhenEmpty()
      throws IOException {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    String dir = "it-delete-flat-nonrec-" + UUID.randomUUID() + "/";
    String child = dir + "child.txt";
    createBlob(bucket, dir, new byte[0]);
    createBlob(bucket, child, TEST_CONTENT);
    URI dirUri = URI.create("gs://" + bucket + "/" + dir);

    // Act & Assert (non-empty)
    DirectoryNotEmptyException nonEmptyEx =
        assertThrows(DirectoryNotEmptyException.class, () -> fileSystem.delete(dirUri, false));
    assertThat(nonEmptyEx.getFile()).isEqualTo("gs://" + bucket + "/" + dir);
    assertThat(storage.get(BlobId.of(bucket, dir))).isNotNull();
    assertThat(storage.get(BlobId.of(bucket, child))).isNotNull();

    // Remove child, then non-recursive delete succeeds on empty placeholder.
    fileSystem.delete(URI.create("gs://" + bucket + "/" + child), /* recursive= */ false);
    fileSystem.delete(dirUri, /* recursive= */ false);
    assertThat(storage.get(BlobId.of(bucket, dir))).isNull();
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatRecursiveNestedDirectory_deletesAllFilesAndPlaceholders() throws IOException {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    String root = "it-delete-flat-rec-" + UUID.randomUUID() + "/";
    String sub = root + "sub/";
    String deep = sub + "deep/";
    String file1 = root + "a.txt";
    String file2 = deep + "b.txt";
    createBlob(bucket, root, new byte[0]);
    createBlob(bucket, sub, new byte[0]);
    createBlob(bucket, deep, new byte[0]);
    createBlob(bucket, file1, TEST_CONTENT);
    createBlob(bucket, file2, TEST_CONTENT);

    // Act
    fileSystem.delete(URI.create("gs://" + bucket + "/" + root), /* recursive= */ true);

    // Assert
    assertThat(storage.get(BlobId.of(bucket, root))).isNull();
    assertThat(storage.get(BlobId.of(bucket, sub))).isNull();
    assertThat(storage.get(BlobId.of(bucket, deep))).isNull();
    assertThat(storage.get(BlobId.of(bucket, file1))).isNull();
    assertThat(storage.get(BlobId.of(bucket, file2))).isNull();
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void delete_hnsNonRecursiveDirectory_enforcesEmptinessAndClearsEmptyFolder() throws IOException {
    // Arrange
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    String root = "it-delete-hns-nonrec-" + UUID.randomUUID() + "/";
    String childFolder = root + "child/";
    registerFolderForCleanup(bucket, root);
    registerFolderForCleanup(bucket, childFolder);
    fileSystem.mkdirs(URI.create("gs://" + bucket + "/" + childFolder));

    // Act & Assert: parent has child folder -> DirectoryNotEmptyException.
    DirectoryNotEmptyException nonEmptyEx =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> fileSystem.delete(URI.create("gs://" + bucket + "/" + root), false));
    assertThat(nonEmptyEx.getFile()).isEqualTo("gs://" + bucket + "/" + root);

    // Delete empty child folder, then empty root folder.
    fileSystem.delete(URI.create("gs://" + bucket + "/" + childFolder), false);
    fileSystem.delete(URI.create("gs://" + bucket + "/" + root), false);

    assertThrows(
        FileNotFoundException.class,
        () -> fileSystem.getFileInfo(URI.create("gs://" + bucket + "/" + root)));
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void delete_hnsNonRecursiveZeroByteLegacyPlaceholder_deletesPlaceholderAndFolder()
      throws IOException {
    // Arrange
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    String folder = "it-delete-hns-legacy0-" + UUID.randomUUID() + "/";
    registerFolderForCleanup(bucket, folder);
    createBlob(bucket, folder, new byte[0]);

    // Act
    fileSystem.delete(URI.create("gs://" + bucket + "/" + folder), /* recursive= */ false);

    // Assert
    assertThat(storage.get(BlobId.of(bucket, folder))).isNull();
    assertThrows(
        FileNotFoundException.class,
        () -> fileSystem.getFileInfo(URI.create("gs://" + bucket + "/" + folder)));
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void delete_hnsNonRecursiveNonEmptyPrefixObject_throwsDirectoryNotEmptyAndPreservesObject()
      throws IOException {
    // Arrange
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    String folder = "it-delete-hns-nonempty-obj-" + UUID.randomUUID() + "/";
    registerFolderForCleanup(bucket, folder);
    createBlob(bucket, folder, TEST_CONTENT);

    // Act & Assert
    DirectoryNotEmptyException nonEmptyEx =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> fileSystem.delete(URI.create("gs://" + bucket + "/" + folder), false));
    assertThat(nonEmptyEx.getFile()).isEqualTo("gs://" + bucket + "/" + folder);
    assertThat(storage.get(BlobId.of(bucket, folder))).isNotNull();
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void delete_hnsRecursiveServerSideEnabled_deletesSubtree() throws IOException {
    // Arrange
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    String root = "it-delete-hns-rec-lro-" + UUID.randomUUID() + "/";
    String sub = root + "sub/";
    String deep = sub + "deep/";
    String file1 = root + "a.txt";
    String file2 = deep + "b.txt";
    registerFolderForCleanup(bucket, root);
    registerFolderForCleanup(bucket, sub);
    registerFolderForCleanup(bucket, deep);
    fileSystem.mkdirs(URI.create("gs://" + bucket + "/" + deep));
    createBlob(bucket, file1, TEST_CONTENT);
    createBlob(bucket, file2, TEST_CONTENT);

    // Act
    fileSystem.delete(URI.create("gs://" + bucket + "/" + root), /* recursive= */ true);

    // Assert
    assertThat(storage.get(BlobId.of(bucket, file1))).isNull();
    assertThat(storage.get(BlobId.of(bucket, file2))).isNull();
    assertThrows(
        FileNotFoundException.class,
        () -> fileSystem.getFileInfo(URI.create("gs://" + bucket + "/" + root)));
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void delete_hnsRecursiveServerSideDisabled_deletesSubtreeClientSide() throws IOException {
    // Arrange
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    String root = "it-delete-hns-rec-client-" + UUID.randomUUID() + "/";
    String sub = root + "sub/";
    String deep = sub + "deep/";
    String file = deep + "b.txt";
    registerFolderForCleanup(bucket, root);
    registerFolderForCleanup(bucket, sub);
    registerFolderForCleanup(bucket, deep);
    fileSystem.mkdirs(URI.create("gs://" + bucket + "/" + deep));
    createBlob(bucket, root, new byte[0]);
    createBlob(bucket, file, TEST_CONTENT);

    try (GcsFileSystemImpl clientSideFs =
        createFileSystem(
            GcsFileSystemOptions.builder().setHnsServerSideRecursiveDeleteEnabled(false))) {
      // Act
      clientSideFs.delete(URI.create("gs://" + bucket + "/" + root), /* recursive= */ true);

      // Assert
      assertThat(storage.get(BlobId.of(bucket, root))).isNull();
      assertThat(storage.get(BlobId.of(bucket, file))).isNull();
      assertThrows(
          FileNotFoundException.class,
          () -> clientSideFs.getFileInfo(URI.create("gs://" + bucket + "/" + root)));
    }
  }

  @Test
  @EnabledIfSystemProperty(named = FLAT_BUCKET_PROPERTY, matches = ".+")
  void delete_flatSpecialCharactersViaItemIdAndEncodedUri_deletesObjectAndDirectory()
      throws IOException {
    // Arrange
    String bucket = System.getProperty(FLAT_BUCKET_PROPERTY);
    String dirName = "it-delete space %25#?-" + UUID.randomUUID() + "/";
    String fileByItemId = dirName + "file 100%#?.txt";
    String fileByUri = dirName + "uri 50%#?.txt";
    createBlob(bucket, dirName, new byte[0]);
    createBlob(bucket, fileByItemId, TEST_CONTENT);
    createBlob(bucket, fileByUri, TEST_CONTENT);
    GcsItemId itemIdTarget =
        GcsItemId.builder().setBucketName(bucket).setObjectName(fileByItemId).build();
    GcsItemId uriTarget =
        GcsItemId.builder().setBucketName(bucket).setObjectName(fileByUri).build();
    GcsItemId dirTarget = GcsItemId.builder().setBucketName(bucket).setObjectName(dirName).build();

    // Act
    fileSystem.delete(itemIdTarget, /* recursive= */ false);
    fileSystem.delete(uriTarget.toUri(), /* recursive= */ false);
    fileSystem.delete(dirTarget.toUri(), /* recursive= */ false);

    // Assert
    assertThat(storage.get(BlobId.of(bucket, fileByItemId))).isNull();
    assertThat(storage.get(BlobId.of(bucket, fileByUri))).isNull();
    assertThat(storage.get(BlobId.of(bucket, dirName))).isNull();
  }

  @Test
  @EnabledIfSystemProperty(named = HNS_BUCKET_PROPERTY, matches = ".+")
  void delete_hnsFolderWithLeadingAndTrailingSpaces_deletesOnlyTargetFolder() throws IOException {
    // Arrange
    String bucket = System.getProperty(HNS_BUCKET_PROPERTY);
    String root = "it-delete-hns-spaces-" + UUID.randomUUID() + "/";
    String paddedFolder = root + " dir /";
    String plainFolder = root + "dir/";
    registerFolderForCleanup(bucket, root);
    registerFolderForCleanup(bucket, paddedFolder);
    registerFolderForCleanup(bucket, plainFolder);
    GcsItemId paddedId =
        GcsItemId.builder().setBucketName(bucket).setObjectName(paddedFolder).build();
    GcsItemId plainId =
        GcsItemId.builder().setBucketName(bucket).setObjectName(plainFolder).build();
    fileSystem.mkdirs(paddedId);
    fileSystem.mkdirs(plainId);

    // Act: delete the space-padded folder via its percent-encoded URI.
    fileSystem.delete(paddedId.toUri(), /* recursive= */ false);

    // Assert: padded folder is gone while sibling plain folder remains intact.
    assertThrows(FileNotFoundException.class, () -> fileSystem.getFileInfo(paddedId));
    assertThat(fileSystem.getFileInfo(plainId).getItemInfo().getItemType())
        .isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);

    // Clean up remaining folders via delete(GcsItemId, true).
    fileSystem.delete(
        GcsItemId.builder().setBucketName(bucket).setObjectName(root).build(),
        /* recursive= */ true);
  }

  @Test
  @EnabledIfSystemProperty(named = PROJECT_ID_PROPERTY, matches = ".+")
  void delete_ephemeralFlatBucket_respectsBucketDeleteEnabledFlag() throws IOException {
    // Arrange
    String bucketName = "it-fs-delete-flat-" + UUID.randomUUID();
    bucketsToCleanup.add(bucketName);
    fileSystem.getGcsClient().createBucket(bucketName);
    createBlob(bucketName, "a/file.txt", TEST_CONTENT);
    URI bucketUri = URI.create("gs://" + bucketName);

    // Non-recursive delete fails while non-empty.
    assertThrows(DirectoryNotEmptyException.class, () -> fileSystem.delete(bucketUri, false));

    // Recursive delete with default isBucketDeleteEnabled=false clears contents but keeps bucket.
    fileSystem.delete(bucketUri, /* recursive= */ true);
    assertThat(storage.get(BlobId.of(bucketName, "a/file.txt"))).isNull();
    assertThat(storage.get(bucketName)).isNotNull();

    // With isBucketDeleteEnabled=true, deletes the now-empty bucket.
    try (GcsFileSystemImpl bucketDeleteFs =
        createFileSystem(GcsFileSystemOptions.builder().setBucketDeleteEnabled(true))) {
      bucketDeleteFs.delete(bucketUri, /* recursive= */ false);
      assertThat(storage.get(bucketName)).isNull();
    }
  }

  @Test
  @EnabledIfSystemProperty(named = PROJECT_ID_PROPERTY, matches = ".+")
  void delete_ephemeralHnsBucketRecursive_bucketDeleteEnabled_clearsAndDeletesBucket()
      throws IOException {
    // Arrange
    String bucketName = "it-fs-delete-hns-" + UUID.randomUUID();
    hnsBucketsToCleanup.add(bucketName);
    fileSystem
        .getGcsClient()
        .createBucket(
            bucketName,
            GcsCreateBucketOptions.builder().setHierarchicalNamespaceEnabled(true).build());
    fileSystem.mkdirs(URI.create("gs://" + bucketName + "/dir/sub/"));
    createBlob(bucketName, "dir/sub/file.txt", TEST_CONTENT);

    try (GcsFileSystemImpl bucketDeleteFs =
        createFileSystem(GcsFileSystemOptions.builder().setBucketDeleteEnabled(true))) {
      // Act
      bucketDeleteFs.delete(URI.create("gs://" + bucketName), /* recursive= */ true);

      // Assert
      assertThat(storage.get(bucketName)).isNull();
    }
  }

  private GcsFileSystemImpl createFileSystem(GcsFileSystemOptions.Builder optionsBuilder) {
    GcsClientOptions.Builder clientBuilder = GcsClientOptions.builder();
    String projectId = System.getProperty(PROJECT_ID_PROPERTY);
    if (projectId != null && !projectId.isEmpty()) {
      clientBuilder.setProjectId(projectId);
    }
    return new GcsFileSystemImpl(
        optionsBuilder.setGcsClientOptions(clientBuilder.build()).build());
  }

  private void createBlob(String bucket, String objectName, byte[] content) {
    BlobId blobId = BlobId.of(bucket, objectName);
    blobsToCleanup.add(blobId);
    storage.create(BlobInfo.newBuilder(blobId).build(), content);
  }

  private void registerFolderForCleanup(String bucket, String folderName) {
    foldersToCleanup.add("projects/_/buckets/" + bucket + "/folders/" + folderName);
  }

  private void cleanupEphemeralBucket(String bucketName, boolean isHns) {
    if (storage == null || storage.get(bucketName) == null) {
      return;
    }
    try {
      for (Blob blob : storage.list(bucketName).iterateAll()) {
        try {
          storage.delete(blob.getBlobId());
        } catch (Exception e) {
          LOG.warn("Failed to delete leftover blob {} in {}", blob.getName(), bucketName, e);
        }
      }
      if (isHns) {
        List<String> bucketFolders = new ArrayList<>();
        try (StorageControlClient controlClient = StorageControlClient.create()) {
          for (Folder folder :
              controlClient
                  .listFolders(
                      ListFoldersRequest.newBuilder()
                          .setParent("projects/_/buckets/" + bucketName)
                          .build())
                  .iterateAll()) {
            bucketFolders.add(folder.getName());
          }
        }
        deleteFoldersDeepestFirst(bucketFolders);
      }
      storage.delete(bucketName);
    } catch (Exception e) {
      LOG.warn("Failed to clean up ephemeral bucket {}", bucketName, e);
    }
  }

  private static void deleteFoldersDeepestFirst(List<String> folderResourceNames) {
    if (folderResourceNames.isEmpty()) {
      return;
    }
    List<String> sorted = new ArrayList<>(folderResourceNames);
    sorted.sort(
        Comparator.comparingInt(
                (String folder) ->
                    CharMatcher.is('/').countIn(UriUtil.removeTrailingSlash(folder)))
            .thenComparingInt(String::length)
            .reversed());
    try (StorageControlClient controlClient = StorageControlClient.create()) {
      for (String resourceName : sorted) {
        try {
          controlClient.deleteFolder(
              DeleteFolderRequest.newBuilder().setName(resourceName).build());
        } catch (Exception e) {
          LOG.debug("Ignoring folder cleanup error for {}", resourceName, e);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to close StorageControlClient during folder cleanup", e);
    }
  }
}
