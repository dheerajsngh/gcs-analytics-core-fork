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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnimplementedException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

class GcsFileSystemDeleteTest {

  private static final String FLAT_BUCKET = "flat-bucket";
  private static final String HNS_BUCKET = "hns-bucket";

  private GcsClient mockGcsClient;
  private GcsFileSystemImpl fileSystem;

  @BeforeEach
  void setUp() throws IOException {
    mockGcsClient = mock(GcsClient.class);
    when(mockGcsClient.getBucketProperties(FLAT_BUCKET))
        .thenReturn(BucketProperties.create(/* hnsEnabled= */ false, /* isRapid= */ false));
    when(mockGcsClient.getBucketProperties(HNS_BUCKET))
        .thenReturn(BucketProperties.create(/* hnsEnabled= */ true, /* isRapid= */ false));
    fileSystem = createFileSystem(defaultSyncOptions());
  }

  @AfterEach
  void tearDown() {
    if (fileSystem != null) {
      fileSystem.close();
    }
  }

  private GcsFileSystemOptions defaultSyncOptions() {
    return GcsFileSystemOptions.builder()
        .setMetadataLookupParallelEnabled(false)
        .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
        .build();
  }

  private GcsFileSystemImpl createFileSystem(GcsFileSystemOptions options) {
    return new GcsFileSystemImpl(mockGcsClient, options);
  }

  @Test
  void delete_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> fileSystem.delete((GcsItemId) null, false));
    verifyNoInteractions(mockGcsClient);
  }

  @Test
  void delete_rootItemId_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> fileSystem.delete(GcsItemId.ROOT, true));

    assertThat(e).hasMessageThat().contains("Cannot delete root path");
    verifyNoInteractions(mockGcsClient);
  }

  @Test
  void delete_rootUri_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class, () -> fileSystem.delete(URI.create("gs:/"), false));
    verifyNoInteractions(mockGcsClient);
  }

  @Test
  void delete_emptyBucketWithObjectName_throwsIllegalArgumentException() {
    GcsItemId malformed = GcsItemId.builder().setBucketName("").setObjectName("dir/file").build();

    assertThrows(IllegalArgumentException.class, () -> fileSystem.delete(malformed, false));
    verifyNoInteractions(mockGcsClient);
  }

  @Test
  void delete_nonExistentObject_throwsFileNotFoundException() throws IOException {
    // Arrange
    GcsItemId itemId = itemId(FLAT_BUCKET, "missing.txt");
    when(mockGcsClient.getGcsItemInfo(itemId))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(itemId));
    when(mockGcsClient.listFirstObjectWithPrefix(itemId.toDirectoryId()))
        .thenReturn(Optional.empty());

    // Act & Assert
    assertThrows(FileNotFoundException.class, () -> fileSystem.delete(itemId, false));
    verify(mockGcsClient, never()).deleteObject(any());
  }

  @Test
  void delete_singleObjectViaUri_deletesObjectWithGeneration() throws IOException {
    // Arrange
    URI uri = URI.create("gs://" + FLAT_BUCKET + "/file.txt");
    GcsItemId lookupId = itemId(FLAT_BUCKET, "file.txt");
    GcsItemId resolvedId = itemIdWithGen(FLAT_BUCKET, "file.txt", 42L);
    when(mockGcsClient.getGcsItemInfo(lookupId)).thenReturn(objectInfo(resolvedId));

    // Act
    fileSystem.delete(uri, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(resolvedId);
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void delete_flatNonRecursiveEmptyPlaceholder_deletesPlaceholderAndRepairsImplicitParent()
      throws IOException {
    // Arrange
    GcsItemId dirLookup = itemId(FLAT_BUCKET, "parent/child/");
    GcsItemId dirResolved = itemIdWithGen(FLAT_BUCKET, "parent/child/", 11L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "parent/");
    GcsItemInfo placeholderInfo = placeholderInfo(dirResolved);
    when(mockGcsClient.listFirstObjectWithPrefix(dirLookup))
        .thenReturn(Optional.of(placeholderInfo));
    when(mockGcsClient.listObjectsPage(dirLookup, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(placeholderInfo).build());
    when(mockGcsClient.getGcsItemInfo(parentDir))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(parentDir));

    // Act
    fileSystem.delete(dirLookup, /* recursive= */ false);

    // Assert
    InOrder order = inOrder(mockGcsClient);
    order.verify(mockGcsClient).deleteObject(dirResolved);
    order.verify(mockGcsClient).createEmptyObject(parentDir);
  }

  @Test
  void delete_flatNonRecursiveEmptyInferred_returnsWithoutDeletingPlaceholder() throws IOException {
    // Arrange
    GcsItemId dirLookup = itemId(FLAT_BUCKET, "dir/");
    when(mockGcsClient.listFirstObjectWithPrefix(dirLookup))
        .thenReturn(Optional.of(objectInfo(itemIdWithGen(FLAT_BUCKET, "dir/gone.txt", 1L))));
    when(mockGcsClient.listObjectsPage(dirLookup, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    fileSystem.delete(dirLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient, never()).deleteObject(any());
  }

  @Test
  void delete_flatNonRecursiveNonEmpty_throwsDirectoryNotEmptyAndDoesNotRepairParent()
      throws IOException {
    // Arrange
    GcsItemId dirLookup = itemId(FLAT_BUCKET, "parent/child/");
    GcsItemInfo placeholderInfo = placeholderInfo(itemIdWithGen(FLAT_BUCKET, "parent/child/", 5L));
    when(mockGcsClient.listFirstObjectWithPrefix(dirLookup))
        .thenReturn(Optional.of(placeholderInfo));
    when(mockGcsClient.listObjectsPage(dirLookup, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(objectInfo(itemIdWithGen(FLAT_BUCKET, "parent/child/a.txt", 6L))))
                .build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(DirectoryNotEmptyException.class, () -> fileSystem.delete(dirLookup, false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + FLAT_BUCKET + "/parent/child/");
    verify(mockGcsClient, never()).deleteObject(any());
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void delete_flatRecursiveNestedTree_deletesFilesThenDeepestMarkersThenSelf() throws IOException {
    // Arrange
    GcsItemId rootDir = itemIdWithGen(FLAT_BUCKET, "a/", 1L);
    GcsItemId nestedDir = itemIdWithGen(FLAT_BUCKET, "a/b/", 2L);
    GcsItemId deepDir = itemIdWithGen(FLAT_BUCKET, "a/b/c/", 3L);
    GcsItemId file1 = itemIdWithGen(FLAT_BUCKET, "a/f1.txt", 4L);
    GcsItemId file2 = itemIdWithGen(FLAT_BUCKET, "a/b/c/f2.txt", 5L);
    GcsItemInfo rootInfo = placeholderInfo(rootDir);

    when(mockGcsClient.listFirstObjectWithPrefix(itemId(FLAT_BUCKET, "a/")))
        .thenReturn(Optional.of(rootInfo));
    when(mockGcsClient.listObjectsPage(
            itemId(FLAT_BUCKET, "a/"), NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(rootInfo)
                .setItems(
                    List.of(
                        objectInfo(file1),
                        placeholderInfo(nestedDir),
                        placeholderInfo(deepDir),
                        objectInfo(file2)))
                .build());
    List<GcsItemId> deleted = new ArrayList<>();
    doAnswer(
            inv -> {
              deleted.add(inv.getArgument(0));
              return null;
            })
        .when(mockGcsClient)
        .deleteObject(any());

    // Act
    fileSystem.delete(URI.create("gs://" + FLAT_BUCKET + "/a/"), /* recursive= */ true);

    // Assert
    assertThat(deleted).containsExactly(file1, file2, deepDir, nestedDir, rootDir).inOrder();
  }

  @Test
  void delete_hnsNonRecursiveEmptyFolder_deletesFolderWithoutParentRepair() throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "parent/child/");
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    when(mockGcsClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    fileSystem.delete(folderId, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteFolder(folderId);
    verify(mockGcsClient, never()).createEmptyObject(any());
    verify(mockGcsClient, never()).getGcsItemInfo(itemId(HNS_BUCKET, "parent/"));
  }

  @Test
  void delete_hnsNonRecursiveNonEmptyPrefixObject_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "dir/");
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    GcsItemInfo nonEmptyPrefixObject =
        GcsItemInfo.builder()
            .setItemId(folderId)
            .setSize(128L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockGcsClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setPrefixObject(nonEmptyPrefixObject).build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(DirectoryNotEmptyException.class, () -> fileSystem.delete(folderId, false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + HNS_BUCKET + "/dir/");
    verify(mockGcsClient, never()).deleteObject(any());
    verify(mockGcsClient, never()).deleteFolder(any());
  }

  @Test
  void delete_hnsNonRecursiveZeroByteLegacyPlaceholder_deletesPlaceholderThenFolder()
      throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "dir/");
    GcsItemId legacyId = itemIdWithGen(HNS_BUCKET, "dir/", 88L);
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    when(mockGcsClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setPrefixObject(placeholderInfo(legacyId)).build());

    // Act
    fileSystem.delete(folderId, /* recursive= */ false);

    // Assert
    InOrder order = inOrder(mockGcsClient);
    order.verify(mockGcsClient).deleteObject(legacyId);
    order.verify(mockGcsClient).deleteFolder(folderId);
  }

  @Test
  void delete_hnsRecursive_usesServerSideDeleteFolderRecursive() throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "dir/");
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));

    // Act
    fileSystem.delete(folderId, /* recursive= */ true);

    // Assert
    verify(mockGcsClient).deleteFolderRecursive(folderId);
    verify(mockGcsClient, never()).listObjectsPage(any(), any());
    verify(mockGcsClient, never()).listFolders(any());
  }

  @Test
  void delete_hnsRecursiveServerSideRejected_fallsBackToClientSide() throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "dir/");
    GcsItemId childFolder = itemId(HNS_BUCKET, "dir/sub/");
    GcsItemId childFile = itemIdWithGen(HNS_BUCKET, "dir/sub/f.txt", 9L);
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    doThrow(
            new DeleteFolderRecursiveRejectedException(
                "Failed to delete folder recursively", mock(UnimplementedException.class)))
        .when(mockGcsClient)
        .deleteFolderRecursive(folderId);
    when(mockGcsClient.listObjectsPage(folderId, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setItems(List.of(objectInfo(childFile))).build());
    when(mockGcsClient.listFolders(folderId))
        .thenReturn(
            List.of(
                GcsItemInfo.createFolder(folderId, 0L, 0L, 1L),
                GcsItemInfo.createFolder(childFolder, 0L, 0L, 1L)));

    // Act
    fileSystem.delete(folderId, /* recursive= */ true);

    // Assert
    InOrder order = inOrder(mockGcsClient);
    order.verify(mockGcsClient).deleteFolderRecursive(folderId);
    order.verify(mockGcsClient).deleteObject(childFile);
    order.verify(mockGcsClient).deleteFolder(childFolder);
    order.verify(mockGcsClient).deleteFolder(folderId);
  }

  @ParameterizedTest
  @MethodSource("nonFallbackRecursiveDeleteErrors")
  void delete_hnsRecursiveNonFallbackErrors_propagatesWithoutClientSideFallback(IOException error)
      throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "dir/");
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    doThrow(error).when(mockGcsClient).deleteFolderRecursive(folderId);

    // Act
    IOException thrown =
        assertThrows(IOException.class, () -> fileSystem.delete(folderId, /* recursive= */ true));

    // Assert
    assertThat(thrown).isSameInstanceAs(error);
    verify(mockGcsClient, never()).listObjectsPage(any(), any());
    verify(mockGcsClient, never()).listFolders(any());
  }

  private static Stream<IOException> nonFallbackRecursiveDeleteErrors() {
    return Stream.of(
        new AccessDeniedException("gs://" + HNS_BUCKET + "/dir/"),
        new InterruptedIOException("interrupted waiting for LRO"),
        new IOException("Timed out waiting to delete folder recursively (operation: op-1)"));
  }

  @Test
  void delete_hnsRecursiveServerSideOptOut_runsClientSideDirectly() throws IOException {
    // Arrange
    try (GcsFileSystemImpl optOutFs =
        createFileSystem(
            defaultSyncOptions().toBuilder()
                .setHnsServerSideRecursiveDeleteEnabled(false)
                .build())) {
      GcsItemId folderId = itemId(HNS_BUCKET, "dir/");
      when(mockGcsClient.getFolderInfo(folderId))
          .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
      when(mockGcsClient.listObjectsPage(folderId, NamespaceStrategy.DELETE_LIST_OPTIONS))
          .thenReturn(GcsListPage.<GcsItemInfo>builder().build());
      when(mockGcsClient.listFolders(folderId))
          .thenReturn(List.of(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L)));

      // Act
      optOutFs.delete(folderId, /* recursive= */ true);

      // Assert
      verify(mockGcsClient, never()).deleteFolderRecursive(any());
      verify(mockGcsClient).deleteFolder(folderId);
    }
  }

  @Test
  void delete_flatBucketNonRecursiveEmpty_bucketDeleteDisabled_skipsDeleteBucket()
      throws IOException {
    // Arrange
    GcsItemId bucketId = GcsItemId.builder().setBucketName(FLAT_BUCKET).build();
    when(mockGcsClient.getBucketInfo(bucketId)).thenReturn(GcsItemInfo.createBucket(bucketId));
    when(mockGcsClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    fileSystem.delete(bucketId, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT);
    verify(mockGcsClient, never()).deleteBucket(any());
  }

  @Test
  void delete_flatBucketNonRecursiveEmpty_bucketDeleteEnabled_callsDeleteBucket()
      throws IOException {
    // Arrange
    try (GcsFileSystemImpl bucketDeleteFs =
        createFileSystem(defaultSyncOptions().toBuilder().setBucketDeleteEnabled(true).build())) {
      GcsItemId bucketId = GcsItemId.builder().setBucketName(FLAT_BUCKET).build();
      when(mockGcsClient.getBucketInfo(bucketId)).thenReturn(GcsItemInfo.createBucket(bucketId));
      when(mockGcsClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT))
          .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

      // Act
      bucketDeleteFs.delete(bucketId, /* recursive= */ false);

      // Assert
      verify(mockGcsClient).deleteBucket(bucketId);
    }
  }

  @Test
  void delete_flatBucketNonRecursiveNonEmpty_throwsDirectoryNotEmptyEvenWhenBucketDeleteEnabled()
      throws IOException {
    // Arrange
    try (GcsFileSystemImpl bucketDeleteFs =
        createFileSystem(defaultSyncOptions().toBuilder().setBucketDeleteEnabled(true).build())) {
      GcsItemId bucketId = GcsItemId.builder().setBucketName(FLAT_BUCKET).build();
      when(mockGcsClient.getBucketInfo(bucketId)).thenReturn(GcsItemInfo.createBucket(bucketId));
      when(mockGcsClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT))
          .thenReturn(
              GcsListPage.<GcsItemInfo>builder()
                  .setItems(List.of(objectInfo(itemIdWithGen(FLAT_BUCKET, "f.txt", 1L))))
                  .build());

      // Act
      DirectoryNotEmptyException e =
          assertThrows(
              DirectoryNotEmptyException.class,
              () -> bucketDeleteFs.delete(bucketId, /* recursive= */ false));

      // Assert
      assertThat(e.getFile()).isEqualTo("gs://" + FLAT_BUCKET);
      verify(mockGcsClient, never()).deleteBucket(any());
    }
  }

  @Test
  void delete_hnsBucketRecursive_bucketDeleteEnabled_clearsClientSideAndDeletesBucket()
      throws IOException {
    // Arrange
    try (GcsFileSystemImpl bucketDeleteFs =
        createFileSystem(defaultSyncOptions().toBuilder().setBucketDeleteEnabled(true).build())) {
      GcsItemId bucketId = GcsItemId.builder().setBucketName(HNS_BUCKET).build();
      GcsItemId fileId = itemIdWithGen(HNS_BUCKET, "a/file.txt", 3L);
      GcsItemId folderA = itemId(HNS_BUCKET, "a/");
      when(mockGcsClient.getBucketInfo(bucketId)).thenReturn(GcsItemInfo.createBucket(bucketId));
      when(mockGcsClient.listObjectsPage(bucketId, NamespaceStrategy.DELETE_LIST_OPTIONS))
          .thenReturn(
              GcsListPage.<GcsItemInfo>builder().setItems(List.of(objectInfo(fileId))).build());
      when(mockGcsClient.listFolders(bucketId))
          .thenReturn(List.of(GcsItemInfo.createFolder(folderA, 0L, 0L, 1L)));

      // Act
      bucketDeleteFs.delete(bucketId, /* recursive= */ true);

      // Assert
      verify(mockGcsClient, never()).deleteFolderRecursive(any());
      InOrder order = inOrder(mockGcsClient);
      order.verify(mockGcsClient).deleteObject(fileId);
      order.verify(mockGcsClient).deleteFolder(folderA);
      order.verify(mockGcsClient).deleteBucket(bucketId);
    }
  }

  @Test
  void delete_hnsBucketRecursive_bucketDeleteDisabled_clearsContentsAndSkipsDeleteBucket()
      throws IOException {
    // Arrange
    GcsItemId bucketId = GcsItemId.builder().setBucketName(HNS_BUCKET).build();
    GcsItemId folderA = itemId(HNS_BUCKET, "a/");
    when(mockGcsClient.getBucketInfo(bucketId)).thenReturn(GcsItemInfo.createBucket(bucketId));
    when(mockGcsClient.listObjectsPage(bucketId, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());
    when(mockGcsClient.listFolders(bucketId))
        .thenReturn(List.of(GcsItemInfo.createFolder(folderA, 0L, 0L, 1L)));

    // Act
    fileSystem.delete(bucketId, /* recursive= */ true);

    // Assert
    verify(mockGcsClient, never()).deleteFolderRecursive(any());
    verify(mockGcsClient).deleteFolder(folderA);
    verify(mockGcsClient, never()).deleteBucket(any());
  }

  @Test
  void delete_flatObjectInImplicitParent_repairsParentDirectory() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/b/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/b/file.txt", 7L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/b/");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(parentDir));

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ false);

    // Assert
    InOrder order = inOrder(mockGcsClient);
    order.verify(mockGcsClient).deleteObject(fileResolved);
    order.verify(mockGcsClient).createEmptyObject(parentDir);
  }

  @Test
  void delete_flatObjectInExplicitParent_doesNotCreateEmptyObject() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 7L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir)).thenReturn(placeholderInfo(parentDir));

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void delete_flatAutoRepairDisabled_doesNotProbeOrRepairParent() throws IOException {
    // Arrange
    try (GcsFileSystemImpl noRepairFs =
        createFileSystem(
            defaultSyncOptions().toBuilder()
                .setAutoRepairImplicitDirectoriesEnabled(false)
                .build())) {
      GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
      GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 7L);
      when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));

      // Act
      noRepairFs.delete(fileLookup, /* recursive= */ false);

      // Assert
      verify(mockGcsClient).deleteObject(fileResolved);
      verify(mockGcsClient, never()).getGcsItemInfo(itemId(FLAT_BUCKET, "a/"));
      verify(mockGcsClient, never()).createEmptyObject(any());
    }
  }

  @Test
  void delete_hnsObjectInSubfolder_skipsParentRepair() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(HNS_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(HNS_BUCKET, "a/file.txt", 7L);
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient, never()).createEmptyObject(any());
    verify(mockGcsClient, never()).getFolderInfo(any());
  }

  @Test
  void delete_flatParentRepairProbeFailsWithIOException_deleteStillSucceeds() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 7L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir))
        .thenThrow(new AccessDeniedException("gs://" + FLAT_BUCKET + "/a/"));

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void delete_flatParentRepairCreateFailsWithIOException_deleteStillSucceeds() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 7L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(parentDir));
    doThrow(new IOException("repair failed")).when(mockGcsClient).createEmptyObject(parentDir);

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient).createEmptyObject(parentDir);
  }

  @Test
  void delete_flatParentRepairProbeThrowsError_rethrowsError() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 7L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
    AssertionError error = new AssertionError("fatal probe error");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir)).thenThrow(error);

    // Act
    AssertionError thrown =
        assertThrows(AssertionError.class, () -> fileSystem.delete(fileLookup, false));

    // Assert
    assertThat(thrown).isSameInstanceAs(error);
  }

  @Test
  void delete_flatParentRepairCreateThrowsError_rethrowsError() throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 7L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
    OutOfMemoryError error = new OutOfMemoryError("fatal repair error");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(parentDir));
    doThrow(error).when(mockGcsClient).createEmptyObject(parentDir);

    // Act
    OutOfMemoryError thrown =
        assertThrows(OutOfMemoryError.class, () -> fileSystem.delete(fileLookup, false));

    // Assert
    assertThat(thrown).isSameInstanceAs(error);
  }

  @Test
  void delete_parallelMetadataLookupEnabled_repairsImplicitParent() throws IOException {
    // Arrange
    try (GcsFileSystemImpl parallelFs =
        createFileSystem(
            GcsFileSystemOptions.builder()
                .setMetadataLookupParallelEnabled(true)
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(2).build())
                .build())) {
      GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
      GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 15L);
      GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
      when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
      when(mockGcsClient.getGcsItemInfo(parentDir))
          .thenThrow(GcsExceptionUtil.createFileNotFoundException(parentDir));

      // Act
      parallelFs.delete(fileLookup, /* recursive= */ false);

      // Assert
      verify(mockGcsClient).deleteObject(fileResolved);
      verify(mockGcsClient).createEmptyObject(parentDir);
    }
  }

  @Test
  void delete_alreadyInterruptedThreadForFile_throwsInterruptedIOExceptionWithZeroRpcs() {
    // Arrange
    GcsItemId fileId = itemId(FLAT_BUCKET, "file.txt");
    Thread.currentThread().interrupt();

    // Act
    boolean stillInterrupted;
    try {
      assertThrows(InterruptedIOException.class, () -> fileSystem.delete(fileId, false));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert
    assertThat(stillInterrupted).isTrue();
    verifyNoInteractions(mockGcsClient);
  }

  @Test
  void delete_interruptedDuringObjectLookup_throwsInterruptedIOExceptionWithoutDeletingObject()
      throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "file.txt", 12L);
    when(mockGcsClient.getGcsItemInfo(fileLookup))
        .thenAnswer(
            inv -> {
              Thread.currentThread().interrupt();
              return objectInfo(fileResolved);
            });

    // Act
    boolean stillInterrupted;
    try {
      assertThrows(InterruptedIOException.class, () -> fileSystem.delete(fileLookup, false));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert
    assertThat(stillInterrupted).isTrue();
    verify(mockGcsClient, never()).deleteObject(any());
  }

  @Test
  void delete_interruptedBeforeDeleteBucket_throwsInterruptedIOExceptionWithoutDeletingBucket()
      throws IOException {
    // Arrange
    try (GcsFileSystemImpl bucketDeleteFs =
        createFileSystem(defaultSyncOptions().toBuilder().setBucketDeleteEnabled(true).build())) {
      GcsItemId bucketId = GcsItemId.builder().setBucketName(FLAT_BUCKET).build();
      when(mockGcsClient.getBucketInfo(bucketId)).thenReturn(GcsItemInfo.createBucket(bucketId));
      when(mockGcsClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT))
          .thenAnswer(
              inv -> {
                Thread.currentThread().interrupt();
                return GcsListPage.<GcsItemInfo>builder().build();
              });

      // Act
      boolean stillInterrupted;
      try {
        assertThrows(
            InterruptedIOException.class,
            () -> bucketDeleteFs.delete(bucketId, /* recursive= */ false));
      } finally {
        stillInterrupted = Thread.interrupted();
      }

      // Assert
      assertThat(stillInterrupted).isTrue();
      verify(mockGcsClient, never()).deleteBucket(any());
    }
  }

  @Test
  void delete_inputWithContentGeneration_stripsGenerationFromLookupAndUsesLiveGeneration()
      throws IOException {
    // Arrange
    GcsItemId inputWithGen = itemIdWithGen(FLAT_BUCKET, "file.txt", 111L);
    GcsItemId strippedLookup = itemId(FLAT_BUCKET, "file.txt");
    GcsItemId liveResolved = itemIdWithGen(FLAT_BUCKET, "file.txt", 222L);
    when(mockGcsClient.getGcsItemInfo(strippedLookup)).thenReturn(objectInfo(liveResolved));

    // Act
    fileSystem.delete(inputWithGen, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).getGcsItemInfo(strippedLookup);
    verify(mockGcsClient).deleteObject(liveResolved);
  }

  @Test
  void delete_missingInputWithContentGeneration_stripsGenerationFromFileNotFoundMessage()
      throws IOException {
    // Arrange
    GcsItemId inputWithGen = itemIdWithGen(FLAT_BUCKET, "missing.txt", 111L);
    GcsItemId strippedLookup = itemId(FLAT_BUCKET, "missing.txt");
    when(mockGcsClient.getGcsItemInfo(strippedLookup))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(strippedLookup));
    when(mockGcsClient.listFirstObjectWithPrefix(strippedLookup.toDirectoryId()))
        .thenReturn(Optional.empty());

    // Act
    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> fileSystem.delete(inputWithGen, false));

    // Assert
    assertThat(e).hasMessageThat().doesNotContain("#111");
    assertThat(e).hasMessageThat().contains("gs://" + FLAT_BUCKET + "/missing.txt");
    verify(mockGcsClient).getGcsItemInfo(strippedLookup);
    verify(mockGcsClient, never()).deleteObject(any());
  }

  @Test
  void delete_itemIdWithSpacesAndSpecialChars_deletesObjectWithoutUriError() throws IOException {
    // Arrange
    String objectName = "my dir/file %100#?[v1].txt";
    GcsItemId lookupId = itemId(FLAT_BUCKET, objectName);
    GcsItemId resolvedId = itemIdWithGen(FLAT_BUCKET, objectName, 31L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "my dir/");
    when(mockGcsClient.getGcsItemInfo(lookupId)).thenReturn(objectInfo(resolvedId));
    when(mockGcsClient.getGcsItemInfo(parentDir)).thenReturn(placeholderInfo(parentDir));

    // Act
    fileSystem.delete(lookupId, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(resolvedId);
  }

  @Test
  void delete_uriWithPercentEncodedSpacesAndSpecialChars_decodesAndDeletesObject()
      throws IOException {
    // Arrange
    URI uri = URI.create("gs://" + FLAT_BUCKET + "/my%20dir/a%25b%3Fc%23d.txt");
    GcsItemId decodedLookup = itemId(FLAT_BUCKET, "my dir/a%b?c#d.txt");
    GcsItemId resolvedId = itemIdWithGen(FLAT_BUCKET, "my dir/a%b?c#d.txt", 44L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "my dir/");
    when(mockGcsClient.getGcsItemInfo(decodedLookup)).thenReturn(objectInfo(resolvedId));
    when(mockGcsClient.getGcsItemInfo(parentDir)).thenReturn(placeholderInfo(parentDir));

    // Act
    fileSystem.delete(uri, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(resolvedId);
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://flat-bucket/a?b", "gs://flat-bucket/a#b"})
  void delete_uriWithRawQueryOrFragment_throwsIllegalArgumentException(String invalidUri) {
    assertThrows(
        IllegalArgumentException.class, () -> fileSystem.delete(URI.create(invalidUri), false));
    verifyNoInteractions(mockGcsClient);
  }

  @Test
  void delete_hnsFolderWithLeadingAndTrailingSpaces_deletesFolderVerbatim() throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, " space dir /");
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    when(mockGcsClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    fileSystem.delete(folderId, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).getFolderInfo(folderId);
    verify(mockGcsClient).deleteFolder(folderId);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "staging/../victim.txt",
        "a/./file.txt",
        "./file.txt",
        "../file.txt",
        "/a/file.txt",
        "a//file.txt"
      })
  void delete_flatObjectWithPoisonedOrMalformedParent_deletesObjectAndSkipsParentRepair(
      String objectName) throws IOException {
    // Arrange
    GcsItemId lookupId = itemId(FLAT_BUCKET, objectName);
    GcsItemId resolvedId = itemIdWithGen(FLAT_BUCKET, objectName, 55L);
    when(mockGcsClient.getGcsItemInfo(lookupId)).thenReturn(objectInfo(resolvedId));

    // Act
    fileSystem.delete(lookupId, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(resolvedId);
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void delete_flatSameNameFileAndDirCollision_withoutTrailingSlashDeletesOnlyFile()
      throws IOException {
    // Arrange: both object "foo" and directory "foo/" exist.
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "foo");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "foo", 61L);
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ true);

    // Assert: object wins over directory; directory listing is never performed.
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient, never()).listObjectsPage(any(), any());
  }

  @Test
  void delete_flatSameNameFileAndDirCollision_withTrailingSlashDeletesOnlyDirectory()
      throws IOException {
    // Arrange: trailing slash targets directory "foo/" even when object "foo" also exists.
    GcsItemId dirLookup = itemId(FLAT_BUCKET, "foo/");
    GcsItemId dirResolved = itemIdWithGen(FLAT_BUCKET, "foo/", 62L);
    GcsItemInfo dirInfo = placeholderInfo(dirResolved);
    when(mockGcsClient.listFirstObjectWithPrefix(dirLookup)).thenReturn(Optional.of(dirInfo));
    when(mockGcsClient.listObjectsPage(dirLookup, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(dirInfo).build());

    // Act
    fileSystem.delete(dirLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(dirResolved);
    verify(mockGcsClient, never()).getGcsItemInfo(itemId(FLAT_BUCKET, "foo"));
  }

  @Test
  void delete_hnsSameNameFileAndDirCollision_withoutTrailingSlashDeletesOnlyFile()
      throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(HNS_BUCKET, "foo");
    GcsItemId fileResolved = itemIdWithGen(HNS_BUCKET, "foo", 71L);
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));

    // Act
    fileSystem.delete(fileLookup, /* recursive= */ true);

    // Assert
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient, never()).deleteFolder(any());
    verify(mockGcsClient, never()).deleteFolderRecursive(any());
  }

  @Test
  void delete_hnsSameNameFileAndDirCollision_withTrailingSlashDeletesOnlyFolder()
      throws IOException {
    // Arrange
    GcsItemId folderId = itemId(HNS_BUCKET, "foo/");
    when(mockGcsClient.getFolderInfo(folderId))
        .thenReturn(GcsItemInfo.createFolder(folderId, 0L, 0L, 1L));
    when(mockGcsClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    fileSystem.delete(folderId, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteFolder(folderId);
    verify(mockGcsClient, never()).getGcsItemInfo(itemId(HNS_BUCKET, "foo"));
    verify(mockGcsClient, never()).deleteObject(any());
  }

  @Test
  void delete_flatDirectoryWithoutTrailingSlash_resolvesAndDeletesPlaceholder() throws IOException {
    // Arrange
    GcsItemId dirNoSlash = itemId(FLAT_BUCKET, "dir");
    GcsItemId dirWithSlash = itemId(FLAT_BUCKET, "dir/");
    GcsItemId dirResolved = itemIdWithGen(FLAT_BUCKET, "dir/", 81L);
    GcsItemInfo dirInfo = placeholderInfo(dirResolved);
    when(mockGcsClient.getGcsItemInfo(dirNoSlash))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(dirNoSlash));
    when(mockGcsClient.listFirstObjectWithPrefix(dirWithSlash)).thenReturn(Optional.of(dirInfo));
    when(mockGcsClient.listObjectsPage(dirWithSlash, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(dirInfo).build());

    // Act
    fileSystem.delete(dirNoSlash, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(dirResolved);
  }

  @Test
  void delete_hnsDirectoryWithoutTrailingSlash_resolvesAndDeletesFolder() throws IOException {
    // Arrange
    GcsItemId folderNoSlash = itemId(HNS_BUCKET, "dir");
    GcsItemId folderWithSlash = itemId(HNS_BUCKET, "dir/");
    when(mockGcsClient.getGcsItemInfo(folderNoSlash))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(folderNoSlash));
    when(mockGcsClient.getFolderInfo(folderNoSlash))
        .thenReturn(GcsItemInfo.createFolder(folderWithSlash, 0L, 0L, 1L));
    when(mockGcsClient.listObjectsPage(eq(folderWithSlash), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    fileSystem.delete(folderNoSlash, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteFolder(folderWithSlash);
  }

  @Test
  void delete_flatParentRepairExecutorShutDown_completesDeleteAndSkipsRepair() throws IOException {
    // Arrange: PathType.DIRECTORY bypasses statusExecutor during lookup, so shutting down the
    // parallel executor triggers RejectedExecutionException specifically on parentProbeFuture.
    GcsFileSystemImpl closedParallelFs =
        createFileSystem(
            GcsFileSystemOptions.builder()
                .setMetadataLookupParallelEnabled(true)
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    closedParallelFs.close();

    GcsItemId dirLookup = itemId(FLAT_BUCKET, "parent/child/");
    GcsItemId dirResolved = itemIdWithGen(FLAT_BUCKET, "parent/child/", 91L);
    GcsItemInfo dirInfo = placeholderInfo(dirResolved);
    when(mockGcsClient.listFirstObjectWithPrefix(dirLookup)).thenReturn(Optional.of(dirInfo));
    when(mockGcsClient.listObjectsPage(dirLookup, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(dirInfo).build());

    // Act
    closedParallelFs.delete(dirLookup, /* recursive= */ false);

    // Assert
    verify(mockGcsClient).deleteObject(dirResolved);
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void delete_flatParentRepairInterrupted_completesDeleteAndPreservesInterruptStatus()
      throws IOException {
    // Arrange
    GcsItemId fileLookup = itemId(FLAT_BUCKET, "a/file.txt");
    GcsItemId fileResolved = itemIdWithGen(FLAT_BUCKET, "a/file.txt", 92L);
    GcsItemId parentDir = itemId(FLAT_BUCKET, "a/");
    when(mockGcsClient.getGcsItemInfo(fileLookup)).thenReturn(objectInfo(fileResolved));
    when(mockGcsClient.getGcsItemInfo(parentDir))
        .thenThrow(new InterruptedIOException("interrupted during repair probe"));

    // Act
    boolean stillInterrupted;
    try {
      fileSystem.delete(fileLookup, /* recursive= */ false);
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert: delete already succeeded, so repair logs and preserves interrupt without failing.
    assertThat(stillInterrupted).isTrue();
    verify(mockGcsClient).deleteObject(fileResolved);
    verify(mockGcsClient, never()).createEmptyObject(any());
  }

  @Test
  void publicSignatures_containNoGuavaTypes() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsFileSystem.class, GcsFileSystemOptions.class, GcsFileSystemOptions.Builder.class);
  }

  private static GcsItemId itemId(String bucket, String objectName) {
    return GcsItemId.builder().setBucketName(bucket).setObjectName(objectName).build();
  }

  private static GcsItemId itemIdWithGen(String bucket, String objectName, long gen) {
    return GcsItemId.builder()
        .setBucketName(bucket)
        .setObjectName(objectName)
        .setContentGeneration(gen)
        .build();
  }

  private static GcsItemInfo objectInfo(GcsItemId itemId) {
    return GcsItemInfo.builder()
        .setItemId(itemId)
        .setSize(10L)
        .setContentGeneration(itemId.getContentGeneration().orElse(1L))
        .setItemType(GcsItemInfo.ItemType.OBJECT)
        .build();
  }

  private static GcsItemInfo placeholderInfo(GcsItemId itemId) {
    return GcsItemInfo.builder()
        .setItemId(itemId)
        .setSize(0L)
        .setContentGeneration(itemId.getContentGeneration().orElse(1L))
        .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
        .build();
  }
}
