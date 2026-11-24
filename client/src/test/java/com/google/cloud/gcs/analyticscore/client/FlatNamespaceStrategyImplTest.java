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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.StorageException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlatNamespaceStrategyImplTest {

  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_DIR = "my-dir";

  private GcsClient mockClient;
  private FlatNamespaceStrategyImpl strategy;

  @BeforeEach
  void setUp() {
    mockClient = mock(GcsClient.class);
    strategy = new FlatNamespaceStrategyImpl(mockClient);
  }

  @Test
  void createDirectory_validItemIdWithoutTrailingSlash_createsEmptyObjectWithTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createEmptyObject(expectedItemId);
  }

  @Test
  void createDirectory_validItemIdWithTrailingSlash_createsEmptyObjectWithTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createEmptyObject(expectedItemId);
  }

  @Test
  void createDirectory_nonEmptyObjectAlreadyExists_throwsFileAlreadyExistsExceptionWithReason()
      throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    FileAlreadyExistsException clientException =
        GcsExceptionUtil.createFileAlreadyExistsException(
            expectedItemId, new StorageException(412, "Precondition Failed"));
    doThrow(clientException).when(mockClient).createEmptyObject(expectedItemId);

    FileAlreadyExistsException e =
        assertThrows(FileAlreadyExistsException.class, () -> strategy.createDirectory(itemId));
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/dir/");
    assertThat(e.getOtherFile()).isNull();
    assertThat(e.getReason())
        .isEqualTo("A non-empty object already exists at the directory marker path");
    assertThat(e).hasCauseThat().isSameInstanceAs(clientException);
  }

  @Test
  void createDirectory_ioException_propagatesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    doThrow(new IOException("Network failure")).when(mockClient).createEmptyObject(expectedItemId);

    assertThat(assertThrows(IOException.class, () -> strategy.createDirectory(itemId)))
        .hasMessageThat()
        .isEqualTo("Network failure");
  }

  @Test
  void createDirectory_nullOrNonObjectItemId_throwsException() {
    GcsItemId bucketItemId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();

    assertThrows(NullPointerException.class, () -> strategy.createDirectory(null));
    assertThrows(IllegalArgumentException.class, () -> strategy.createDirectory(bucketItemId));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "//", "/dir/subdir", "dir//subdir", "dir/subdir//"})
  void createDirectory_leadingOrConsecutiveSlashes_throwsIllegalArgumentException(
      String invalidObjectName) {
    GcsItemId invalidItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(invalidObjectName).build();

    assertThrows(IllegalArgumentException.class, () -> strategy.createDirectory(invalidItemId));
    verifyNoInteractions(mockClient);
  }

  @Test
  void getDirectoryInfo_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> strategy.getDirectoryInfo(null));
  }

  @Test
  void getDirectoryInfo_placeholderObjectListed_returnsPlaceholderObjectMetadata()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirId.toDirectoryId();
    GcsItemInfo placeholderItem =
        GcsItemInfo.builder()
            .setItemId(
                GcsItemId.builder()
                    .setBucketName(TEST_BUCKET)
                    .setObjectName(TEST_DIR + "/")
                    .setContentGeneration(123L)
                    .build())
            .setSize(0L)
            .setContentGeneration(123L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId)))
        .thenReturn(Optional.of(placeholderItem));

    GcsItemInfo result = strategy.getDirectoryInfo(dirId);

    assertThat(result.getItemType()).isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
    assertThat(result.getItemId().getBucketName()).isEqualTo(TEST_BUCKET);
    assertThat(result.getItemId().getObjectName()).hasValue(TEST_DIR + "/");
    assertThat(result.getItemId().getContentGeneration()).hasValue(123L);
    assertThat(result.getContentGeneration()).hasValue(123L);
    assertThat(result.getSize()).isEqualTo(0L);
  }

  @Test
  void getDirectoryInfo_childObjectListed_returnsInferredDirectoryWithoutGeneration()
      throws IOException {
    GcsItemId dirIdWithGeneration =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_DIR)
            .setContentGeneration(42L)
            .build();
    GcsItemId prefixId = dirIdWithGeneration.toDirectoryId();
    GcsItemInfo childItem = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId))).thenReturn(Optional.of(childItem));

    GcsItemInfo result = strategy.getDirectoryInfo(dirIdWithGeneration);

    assertThat(result.getItemType()).isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
    assertThat(result.getItemId()).isEqualTo(prefixId);
    assertThat(result.getItemId().getContentGeneration()).isEmpty();
    assertThat(result.getContentGeneration()).isEmpty();
    assertThat(result.getSize()).isEqualTo(0L);
  }

  @Test
  void getDirectoryInfo_implicitDirectoryDoesNotExist_throwsFileNotFoundException()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirId.toDirectoryId();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId))).thenReturn(Optional.empty());

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(dirId));

    assertThat(e)
        .hasMessageThat()
        .contains(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_DIR);
    assertThat(e.getCause()).isInstanceOf(StorageException.class);
    assertThat(((StorageException) e.getCause()).getCode()).isEqualTo(404);
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }

  @Test
  void getDirectoryInfo_listFirstObjectWithPrefixThrowsIOException_throwsIOException()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirId.toDirectoryId();
    when(mockClient.listFirstObjectWithPrefix(eq(prefixId)))
        .thenThrow(new IOException("GCS listing failure"));

    IOException e = assertThrows(IOException.class, () -> strategy.getDirectoryInfo(dirId));

    assertThat(e).hasMessageThat().contains("GCS listing failure");
    verify(mockClient).listFirstObjectWithPrefix(prefixId);
  }

  @Test
  void getDirectoryInfo_inferImplicitDirectoriesFalse_placeholderNotFound_preservesInputIdInError()
      throws IOException {
    GcsItemId dirIdWithoutSlash = createGcsItemId(TEST_DIR);
    GcsItemId prefixId = dirIdWithoutSlash.toDirectoryId();
    when(mockClient.getGcsItemInfo(eq(prefixId)))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(prefixId));

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () ->
                strategy.getDirectoryInfo(
                    dirIdWithoutSlash, /* inferImplicitDirectories= */ false));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_DIR);
    verify(mockClient).getGcsItemInfo(prefixId);
  }

  @Test
  void
      getDirectoryInfo_inferImplicitDirectoriesFalseAndInputAlreadyHasTrailingSlash_rethrowsOriginalException()
          throws IOException {
    GcsItemId dirIdWithSlash = createGcsItemId(TEST_DIR + "/");
    FileNotFoundException expected = GcsExceptionUtil.createFileNotFoundException(dirIdWithSlash);
    when(mockClient.getGcsItemInfo(eq(dirIdWithSlash))).thenThrow(expected);

    FileNotFoundException actual =
        assertThrows(
            FileNotFoundException.class,
            () -> strategy.getDirectoryInfo(dirIdWithSlash, /* inferImplicitDirectories= */ false));

    assertThat(actual).isSameInstanceAs(expected);
    verify(mockClient).getGcsItemInfo(dirIdWithSlash);
  }

  @Test
  void listDirectory_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> strategy.listDirectory(null));
  }

  @Test
  void listDirectory_childrenListed_returnsObjectsAndInferredDirectories() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    GcsItemInfo file = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    GcsItemId implicitDirId = createGcsItemId(TEST_DIR + "/implicit/");
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(file))
                .setPrefixes(List.of(implicitDirId))
                .build());

    List<GcsItemInfo> children = strategy.listDirectory(dirId);

    assertThat(children)
        .containsExactly(file, GcsItemInfo.createInferredDirectory(implicitDirId))
        .inOrder();
  }

  @Test
  void listDirectory_onlyOwnPlaceholderListed_returnsEmptyList() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(GcsItemInfo.createPlaceholderDirectory(dirId))
                .build());

    List<GcsItemInfo> children = strategy.listDirectory(dirId);

    assertThat(children).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {TEST_DIR, TEST_DIR + "/"})
  void listDirectory_nothingListedForObjectPath_throwsFileNotFoundExceptionNamingItemId(
      String objectName) throws IOException {
    GcsItemId itemId = createGcsItemId(objectName);
    when(mockClient.listObjectsPage(itemId.toDirectoryId(), GcsListOptions.LIST_DIRECTORY))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.listDirectory(itemId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + objectName);
  }

  @Test
  void listDirectory_nothingListedForBucket_returnsEmptyList() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    List<GcsItemInfo> children = strategy.listDirectory(bucketId);

    assertThat(children).isEmpty();
  }

  @Test
  void listDirectory_bucketDoesNotExist_propagatesFileNotFoundException() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    FileNotFoundException expected = GcsExceptionUtil.createBucketNotFoundException(TEST_BUCKET);
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY)).thenThrow(expected);

    FileNotFoundException actual =
        assertThrows(FileNotFoundException.class, () -> strategy.listDirectory(dirId));

    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  void listDirectory_nothingListed_doesNotLookUpFolder() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    assertThrows(FileNotFoundException.class, () -> strategy.listDirectory(dirId));

    verify(mockClient, never()).getFolderInfo(any());
  }

  @Test
  void shouldRepairImplicitParentDirectory_returnsTrue() {
    assertThat(strategy.shouldRepairImplicitParentDirectory()).isTrue();
  }

  @Test
  void deleteDirectory_nonRecursiveEmptyPlaceholder_deletesPlaceholderWithGeneration()
      throws IOException {
    // Arrange
    GcsItemId dirId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_DIR + "/")
            .setContentGeneration(123L)
            .build();
    GcsItemInfo dirInfo =
        GcsItemInfo.builder()
            .setItemId(dirId)
            .setSize(0L)
            .setContentGeneration(123L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockClient.listObjectsPage(dirId.toDirectoryId(), GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(dirInfo).build());

    // Act
    strategy.deleteDirectory(dirInfo, /* recursive= */ false);

    // Assert
    verify(mockClient).deleteObject(dirId);
  }

  @Test
  void deleteDirectory_nonRecursiveEmptyInferred_doesNotDeletePlaceholder() throws IOException {
    // Arrange
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    GcsItemInfo inferredDir = GcsItemInfo.createInferredDirectory(dirId);
    when(mockClient.listObjectsPage(dirId, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    strategy.deleteDirectory(inferredDir, /* recursive= */ false);

    // Assert
    verify(mockClient, never()).deleteObject(any());
  }

  @Test
  void deleteDirectory_nonRecursiveNonEmptyOnSecondPage_paginatesAndThrowsDirectoryNotEmpty()
      throws IOException {
    // Arrange
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    GcsItemInfo dirInfo = GcsItemInfo.createPlaceholderDirectory(dirId);
    GcsListPage<GcsItemInfo> firstPage =
        GcsListPage.<GcsItemInfo>builder()
            .setPrefixObject(dirInfo)
            .setNextPageToken("token-2")
            .build();
    GcsListPage<GcsItemInfo> secondPage =
        GcsListPage.<GcsItemInfo>builder()
            .setPrefixes(List.of(createGcsItemId(TEST_DIR + "/sub/")))
            .build();
    when(mockClient.listObjectsPage(dirId, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(firstPage);
    when(mockClient.listObjectsPage(
            dirId, GcsListOptions.EMPTINESS_CHECK_FLAT.toBuilder().setPageToken("token-2").build()))
        .thenReturn(secondPage);

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteDirectory(dirInfo, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_DIR + "/");
    verify(mockClient, never()).deleteObject(any());
  }

  @Test
  void deleteDirectory_nonRecursiveInterrupted_throwsInterruptedIOException() {
    // Arrange
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    GcsItemInfo dirInfo = GcsItemInfo.createPlaceholderDirectory(dirId);
    Thread.currentThread().interrupt();

    // Act
    boolean stillInterrupted;
    try {
      assertThrows(
          InterruptedIOException.class,
          () -> strategy.deleteDirectory(dirInfo, /* recursive= */ false));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert
    assertThat(stillInterrupted).isTrue();
    verifyNoInteractions(mockClient);
  }

  @Test
  void deleteDirectory_nonRecursiveInterruptedAfterListing_skipsDeleteObjectAndPreservesInterrupt()
      throws IOException {
    // Arrange
    GcsItemId dirId = createGcsItemIdWithGen(TEST_DIR + "/", 12L);
    GcsItemInfo dirInfo = createPlaceholderInfo(dirId);
    when(mockClient.listObjectsPage(dirId.toDirectoryId(), GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenAnswer(
            inv -> {
              Thread.currentThread().interrupt();
              return GcsListPage.<GcsItemInfo>builder().setPrefixObject(dirInfo).build();
            });

    // Act
    boolean stillInterrupted;
    try {
      assertThrows(
          InterruptedIOException.class,
          () -> strategy.deleteDirectory(dirInfo, /* recursive= */ false));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert
    assertThat(stillInterrupted).isTrue();
    verify(mockClient, never()).deleteObject(any());
  }

  @Test
  void deleteDirectory_recursiveNestedFilesAndPlaceholders_deletesFilesThenDeepestMarkersThenSelf()
      throws IOException {
    // Arrange
    FlatNamespaceStrategyImpl syncStrategy =
        new FlatNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId rootDir = createGcsItemIdWithGen("root/", 1L);
    GcsItemId fileTop = createGcsItemIdWithGen("root/a.txt", 2L);
    GcsItemId subDir = createGcsItemIdWithGen("root/sub/", 3L);
    GcsItemId fileSub = createGcsItemIdWithGen("root/sub/b.txt", 4L);
    GcsItemId deepDir = createGcsItemIdWithGen("root/sub/deep/", 5L);
    GcsItemId fileDeep = createGcsItemIdWithGen("root/sub/deep/c.txt", 6L);
    GcsItemInfo rootInfo = createPlaceholderInfo(rootDir);

    GcsListPage<GcsItemInfo> page1 =
        GcsListPage.<GcsItemInfo>builder()
            .setPrefixObject(rootInfo)
            .setItems(List.of(createObjectInfo(fileTop), createPlaceholderInfo(subDir)))
            .setNextPageToken("page-2")
            .build();
    GcsListPage<GcsItemInfo> page2 =
        GcsListPage.<GcsItemInfo>builder()
            .setItems(
                List.of(
                    createObjectInfo(fileSub),
                    createPlaceholderInfo(deepDir),
                    createObjectInfo(fileDeep)))
            .build();
    when(mockClient.listObjectsPage(rootDir.toDirectoryId(), NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(page1);
    when(mockClient.listObjectsPage(
            rootDir.toDirectoryId(),
            NamespaceStrategy.DELETE_LIST_OPTIONS.toBuilder().setPageToken("page-2").build()))
        .thenReturn(page2);

    List<GcsItemId> deleteOrder = new ArrayList<>();
    doAnswer(
            invocation -> {
              deleteOrder.add(invocation.getArgument(0));
              return null;
            })
        .when(mockClient)
        .deleteObject(any());

    // Act
    syncStrategy.deleteDirectory(rootInfo, /* recursive= */ true);

    // Assert: files first, then depth-2 placeholder, then depth-1 placeholder, then self marker.
    assertThat(deleteOrder)
        .containsExactly(fileTop, fileSub, fileDeep, deepDir, subDir, rootDir)
        .inOrder();
  }

  @Test
  void deleteDirectory_recursiveChildDeleteFails_doesNotDeleteSelfPlaceholder() throws IOException {
    // Arrange
    FlatNamespaceStrategyImpl syncStrategy =
        new FlatNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId rootDir = createGcsItemIdWithGen("root/", 1L);
    GcsItemId childFile = createGcsItemIdWithGen("root/file.txt", 2L);
    GcsItemInfo rootInfo = createPlaceholderInfo(rootDir);
    when(mockClient.listObjectsPage(rootDir.toDirectoryId(), NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(rootInfo)
                .setItems(List.of(createObjectInfo(childFile)))
                .build());
    doThrow(new IOException("delete failed")).when(mockClient).deleteObject(childFile);

    // Act
    IOException e =
        assertThrows(
            IOException.class, () -> syncStrategy.deleteDirectory(rootInfo, /* recursive= */ true));

    // Assert
    assertThat(e).hasMessageThat().isEqualTo("delete failed");
    verify(mockClient, never()).deleteObject(rootDir);
  }

  @Test
  void deleteBucketContents_nonRecursiveEmpty_succeeds() throws IOException {
    // Arrange
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    strategy.deleteBucketContents(bucketId, /* recursive= */ false);

    // Assert
    verify(mockClient).listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT);
    verify(mockClient, never()).deleteObject(any());
  }

  @Test
  void deleteBucketContents_nonRecursiveNonEmpty_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_FLAT))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(createGcsItemInfo("obj.txt", 10L)))
                .build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteBucketContents(bucketId, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET);
  }

  @Test
  void deleteBucketContents_recursive_deletesFilesBeforePlaceholdersDeepestFirst()
      throws IOException {
    // Arrange
    FlatNamespaceStrategyImpl syncStrategy =
        new FlatNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId dirA = createGcsItemIdWithGen("a/", 1L);
    GcsItemId dirAB = createGcsItemIdWithGen("a/b/", 2L);
    GcsItemId file = createGcsItemIdWithGen("a/b/file.txt", 3L);
    when(mockClient.listObjectsPage(bucketId, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        createPlaceholderInfo(dirA),
                        createPlaceholderInfo(dirAB),
                        createObjectInfo(file)))
                .build());
    List<GcsItemId> deleteOrder = new ArrayList<>();
    doAnswer(
            invocation -> {
              deleteOrder.add(invocation.getArgument(0));
              return null;
            })
        .when(mockClient)
        .deleteObject(any());

    // Act
    syncStrategy.deleteBucketContents(bucketId, /* recursive= */ true);

    // Assert
    assertThat(deleteOrder).containsExactly(file, dirAB, dirA).inOrder();
  }

  private static GcsItemId createGcsItemId(String objectName) {
    return GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemId createGcsItemIdWithGen(String objectName, long generation) {
    return GcsItemId.builder()
        .setBucketName(TEST_BUCKET)
        .setObjectName(objectName)
        .setContentGeneration(generation)
        .build();
  }

  private static GcsItemInfo createGcsItemInfo(String objectName, long size) {
    return GcsItemInfo.builder().setItemId(createGcsItemId(objectName)).setSize(size).build();
  }

  private static GcsItemInfo createObjectInfo(GcsItemId itemId) {
    return GcsItemInfo.builder()
        .setItemId(itemId)
        .setSize(10L)
        .setContentGeneration(itemId.getContentGeneration().orElse(1L))
        .setItemType(GcsItemInfo.ItemType.OBJECT)
        .build();
  }

  private static GcsItemInfo createPlaceholderInfo(GcsItemId itemId) {
    return GcsItemInfo.builder()
        .setItemId(itemId)
        .setSize(0L)
        .setContentGeneration(itemId.getContentGeneration().orElse(1L))
        .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
        .build();
  }
}
