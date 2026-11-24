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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

class HierarchicalNamespaceStrategyImplTest {

  private static final String TEST_BUCKET = "test-hns-bucket";
  private static final String TEST_FOLDER = "my-folder";

  private GcsClient mockClient;
  private HierarchicalNamespaceStrategyImpl strategy;

  @BeforeEach
  void setUp() {
    mockClient = mock(GcsClient.class);
    strategy = new HierarchicalNamespaceStrategyImpl(mockClient);
  }

  @Test
  void createDirectory_validItemIdWithTrailingSlash_createsFolderWithoutTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createFolder(expectedItemId, true);
  }

  @Test
  void createDirectory_validItemIdWithoutTrailingSlash_createsFolderWithoutTrailingSlash()
      throws IOException {
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/subdir").build();

    strategy.createDirectory(itemId);

    verify(mockClient).createFolder(expectedItemId, true);
  }

  @Test
  void createDirectory_alreadyExists_suppressesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    doThrow(new FileAlreadyExistsException("dir"))
        .when(mockClient)
        .createFolder(expectedItemId, true);

    assertDoesNotThrow(() -> strategy.createDirectory(itemId));
  }

  @Test
  void createDirectory_ioException_propagatesException() throws IOException {
    GcsItemId itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir/").build();
    GcsItemId expectedItemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName("dir").build();
    doThrow(new IOException("Network failure")).when(mockClient).createFolder(expectedItemId, true);

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
  void getDirectoryInfo_folderExists_returnsFolderItemInfo() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    GcsItemId folderDirId = folderId.toDirectoryId();
    GcsItemInfo folderInfo =
        GcsItemInfo.builder()
            .setItemId(folderDirId)
            .setSize(0L)
            .setItemType(GcsItemInfo.ItemType.NATIVE_FOLDER)
            .build();
    when(mockClient.getFolderInfo(eq(folderId))).thenReturn(folderInfo);

    GcsItemInfo result = strategy.getDirectoryInfo(folderId);

    assertThat(result).isSameInstanceAs(folderInfo);
  }

  @Test
  void getDirectoryInfo_folderNotFound_throwsFileNotFoundException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    when(mockClient.getFolderInfo(eq(folderId)))
        .thenThrow(GcsExceptionUtil.createFileNotFoundException(folderId));

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.getDirectoryInfo(folderId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_FOLDER);
    verify(mockClient).getFolderInfo(folderId);
  }

  @Test
  void getDirectoryInfo_getFolderInfoThrowsIOException_throwsIOException() throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    when(mockClient.getFolderInfo(eq(folderId)))
        .thenThrow(new IOException("GCS folder lookup failure"));

    IOException e = assertThrows(IOException.class, () -> strategy.getDirectoryInfo(folderId));

    assertThat(e).hasMessageThat().contains("GCS folder lookup failure");
  }

  @Test
  void getDirectoryInfo_inferImplicitDirectoriesFalse_stillQueriesNativeFolder()
      throws IOException {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_FOLDER).build();
    GcsItemInfo folderInfo =
        GcsItemInfo.builder()
            .setItemId(folderId.toDirectoryId())
            .setSize(0L)
            .setItemType(GcsItemInfo.ItemType.NATIVE_FOLDER)
            .build();
    when(mockClient.getFolderInfo(eq(folderId))).thenReturn(folderInfo);

    GcsItemInfo result = strategy.getDirectoryInfo(folderId, /* inferImplicitDirectories= */ false);

    assertThat(result).isSameInstanceAs(folderInfo);
    verify(mockClient).getFolderInfo(folderId);
  }

  @Test
  void listDirectory_nullItemId_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> strategy.listDirectory(null));
  }

  @Test
  void listDirectory_childrenListed_returnsObjectsAndFoldersAsInferredDirectories()
      throws IOException {
    GcsItemId folderId = createGcsItemId(TEST_FOLDER + "/");
    GcsItemInfo file =
        GcsItemInfo.builder()
            .setItemId(createGcsItemId(TEST_FOLDER + "/file.txt"))
            .setSize(100L)
            .build();
    GcsItemId childFolderId = createGcsItemId(TEST_FOLDER + "/child/");
    when(mockClient.listObjectsPage(folderId, GcsListOptions.LIST_DIRECTORY_HNS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(file))
                .setPrefixes(List.of(childFolderId))
                .build());

    List<GcsItemInfo> children = strategy.listDirectory(folderId);

    assertThat(children)
        .containsExactly(file, GcsItemInfo.createInferredDirectory(childFolderId))
        .inOrder();
  }

  @Test
  void listDirectory_childrenListed_doesNotLookUpFolder() throws IOException {
    GcsItemId folderId = createGcsItemId(TEST_FOLDER + "/");
    when(mockClient.listObjectsPage(folderId, GcsListOptions.LIST_DIRECTORY_HNS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixes(List.of(createGcsItemId(TEST_FOLDER + "/child/")))
                .build());

    strategy.listDirectory(folderId);

    verify(mockClient, never()).getFolderInfo(any());
  }

  @Test
  void listDirectory_onlyOwnPlaceholderListed_returnsEmptyListWithoutFolderLookup()
      throws IOException {
    GcsItemId folderId = createGcsItemId(TEST_FOLDER + "/");
    when(mockClient.listObjectsPage(folderId, GcsListOptions.LIST_DIRECTORY_HNS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(GcsItemInfo.createPlaceholderDirectory(folderId))
                .build());

    List<GcsItemInfo> children = strategy.listDirectory(folderId);

    assertThat(children).isEmpty();
    verify(mockClient, never()).getFolderInfo(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {TEST_FOLDER, TEST_FOLDER + "/"})
  void listDirectory_nothingListedAndFolderExists_returnsEmptyList(String objectName)
      throws IOException {
    GcsItemId itemId = createGcsItemId(objectName);
    when(mockClient.listObjectsPage(itemId.toDirectoryId(), GcsListOptions.LIST_DIRECTORY_HNS))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());
    when(mockClient.getFolderInfo(itemId))
        .thenReturn(GcsItemInfo.createFolder(itemId.toDirectoryId(), 1L, 1L, 1L));

    List<GcsItemInfo> children = strategy.listDirectory(itemId);

    assertThat(children).isEmpty();
  }

  @Test
  void listDirectory_nothingListedAndFolderMissing_propagatesFileNotFoundException()
      throws IOException {
    GcsItemId itemId = createGcsItemId(TEST_FOLDER);
    FileNotFoundException expected = GcsExceptionUtil.createFileNotFoundException(itemId);
    when(mockClient.listObjectsPage(itemId.toDirectoryId(), GcsListOptions.LIST_DIRECTORY_HNS))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());
    when(mockClient.getFolderInfo(itemId)).thenThrow(expected);

    FileNotFoundException actual =
        assertThrows(FileNotFoundException.class, () -> strategy.listDirectory(itemId));

    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  void listDirectory_nothingListedForBucket_returnsEmptyListWithoutFolderLookup()
      throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.LIST_DIRECTORY_HNS))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    List<GcsItemInfo> children = strategy.listDirectory(bucketId);

    assertThat(children).isEmpty();
    verify(mockClient, never()).getFolderInfo(any());
  }

  @Test
  void shouldRepairImplicitParentDirectory_returnsFalse() {
    assertThat(strategy.shouldRepairImplicitParentDirectory()).isFalse();
  }

  @Test
  void deleteDirectory_nonRecursiveEmptyFolder_deletesFolder() throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    strategy.deleteDirectory(folderInfo, /* recursive= */ false);

    // Assert
    verify(mockClient).deleteFolder(folderId);
    verify(mockClient, never()).deleteObject(any());
  }

  @Test
  void deleteDirectory_nonRecursiveHasChildObject_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemInfo childObject =
        GcsItemInfo.builder()
            .setItemId(createFolderId(TEST_FOLDER + "/file.txt"))
            .setSize(5L)
            .build();
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setItems(List.of(childObject)).build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_FOLDER + "/");
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_nonRecursiveHasChildSubfolderPrefix_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixes(List.of(createFolderId(TEST_FOLDER + "/child/")))
                .build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_FOLDER + "/");
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_nonRecursiveZeroByteLegacyPrefixObject_deletesObjectBeforeFolder()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemId legacyId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName(TEST_FOLDER + "/")
            .setContentGeneration(77L)
            .build();
    GcsItemInfo legacyPlaceholder =
        GcsItemInfo.builder()
            .setItemId(legacyId)
            .setSize(0L)
            .setContentGeneration(77L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(legacyPlaceholder).build());

    // Act
    strategy.deleteDirectory(folderInfo, /* recursive= */ false);

    // Assert
    InOrder order = inOrder(mockClient);
    order.verify(mockClient).deleteObject(legacyId);
    order.verify(mockClient).deleteFolder(folderId);
  }

  @Test
  void deleteDirectory_nonRecursiveZeroByteLegacyPrefixWithChildOnSecondPage_doesNotDeleteLegacy()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemInfo legacyPlaceholder = GcsItemInfo.createPlaceholderDirectory(folderId);
    GcsListOptions page1Options =
        GcsListOptions.LIST_DIRECTORY_HNS.toBuilder()
            .setPageSize(2)
            .setFields(
                GcsListOptions.ListField.NAME,
                GcsListOptions.ListField.SIZE,
                GcsListOptions.ListField.GENERATION)
            .build();
    GcsListOptions page2Options = page1Options.toBuilder().setPageToken("next").build();
    when(mockClient.listObjectsPage(folderId, page1Options))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(legacyPlaceholder)
                .setNextPageToken("next")
                .build());
    when(mockClient.listObjectsPage(folderId, page2Options))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixes(List.of(createFolderId(TEST_FOLDER + "/sub/")))
                .build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_FOLDER + "/");
    verify(mockClient, never()).deleteObject(any());
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_nonRecursiveNonEmptyPrefixObject_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemInfo nonEmptyPrefixObject =
        GcsItemInfo.builder()
            .setItemId(folderId)
            .setSize(42L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setPrefixObject(nonEmptyPrefixObject).build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_FOLDER + "/");
    verify(mockClient, never()).deleteObject(any());
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_nonRecursiveUnresolvedSizePrefixObject_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange: size == -1L is the unresolved sentinel and must fail closed as non-empty.
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemInfo unresolvedSizePrefixObject =
        GcsItemInfo.builder()
            .setItemId(folderId)
            .setSize(-1L)
            .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
            .build();
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setPrefixObject(unresolvedSizePrefixObject).build());

    // Act
    DirectoryNotEmptyException e =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));

    // Assert
    assertThat(e.getFile()).isEqualTo("gs://" + TEST_BUCKET + "/" + TEST_FOLDER + "/");
    verify(mockClient, never()).deleteObject(any());
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_nonRecursiveInterruptedAfterListing_skipsDeleteObjectAndDeleteFolder()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemInfo legacyPlaceholder = GcsItemInfo.createPlaceholderDirectory(folderId);
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenAnswer(
            inv -> {
              Thread.currentThread().interrupt();
              return GcsListPage.<GcsItemInfo>builder().setPrefixObject(legacyPlaceholder).build();
            });

    // Act
    boolean stillInterrupted;
    try {
      assertThrows(
          InterruptedIOException.class,
          () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert
    assertThat(stillInterrupted).isTrue();
    verify(mockClient, never()).deleteObject(any());
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_nonRecursiveInterruptedAfterLegacyObjectDelete_skipsDeleteFolder()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);
    GcsItemInfo legacyPlaceholder = GcsItemInfo.createPlaceholderDirectory(folderId);
    when(mockClient.listObjectsPage(eq(folderId), any(GcsListOptions.class)))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(legacyPlaceholder).build());
    doAnswer(
            inv -> {
              Thread.currentThread().interrupt();
              return null;
            })
        .when(mockClient)
        .deleteObject(folderId);

    // Act
    boolean stillInterrupted;
    try {
      assertThrows(
          InterruptedIOException.class,
          () -> strategy.deleteDirectory(folderInfo, /* recursive= */ false));
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    // Assert
    assertThat(stillInterrupted).isTrue();
    verify(mockClient).deleteObject(folderId);
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteDirectory_recursiveServerSideEnabled_usesDeleteFolderRecursive() throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    GcsItemInfo folderInfo = GcsItemInfo.createFolder(folderId, 0L, 0L, 1L);

    // Act
    strategy.deleteDirectory(folderInfo, /* recursive= */ true);

    // Assert
    verify(mockClient).deleteFolderRecursive(folderId);
    verify(mockClient, never()).listObjectsPage(any(), any());
    verify(mockClient, never()).listFolders(any());
  }

  @Test
  void deleteDirectory_recursiveInitialRpcRejected_fallsBackToClientSide() throws IOException {
    // Arrange
    HierarchicalNamespaceStrategyImpl syncStrategy =
        new HierarchicalNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId rootFolder = createFolderId("root/");
    GcsItemId subFolder = createFolderId("root/sub/");
    GcsItemId deepFolder = createFolderId("root/sub/deep/");
    GcsItemId legacyObject =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("root/")
            .setContentGeneration(10L)
            .build();
    GcsItemId fileObject =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("root/sub/deep/file.txt")
            .setContentGeneration(20L)
            .build();
    doThrow(
            new DeleteFolderRecursiveRejectedException(
                "Failed to delete folder recursively: gs://" + TEST_BUCKET + "/root/",
                mock(UnimplementedException.class)))
        .when(mockClient)
        .deleteFolderRecursive(rootFolder);
    when(mockClient.listObjectsPage(rootFolder, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(
                    GcsItemInfo.builder()
                        .setItemId(legacyObject)
                        .setSize(0L)
                        .setContentGeneration(10L)
                        .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
                        .build())
                .setItems(
                    List.of(
                        GcsItemInfo.builder()
                            .setItemId(fileObject)
                            .setSize(5L)
                            .setContentGeneration(20L)
                            .build()))
                .build());
    when(mockClient.listFolders(rootFolder))
        .thenReturn(
            List.of(
                GcsItemInfo.createFolder(rootFolder, 0L, 0L, 1L),
                GcsItemInfo.createFolder(subFolder, 0L, 0L, 1L),
                GcsItemInfo.createFolder(deepFolder, 0L, 0L, 1L)));

    List<String> operations = new ArrayList<>();
    doAnswer(
            inv -> {
              operations.add(
                  "deleteObject:" + inv.getArgument(0, GcsItemId.class).getObjectName().get());
              return null;
            })
        .when(mockClient)
        .deleteObject(any());
    doAnswer(
            inv -> {
              operations.add(
                  "deleteFolder:" + inv.getArgument(0, GcsItemId.class).getObjectName().get());
              return null;
            })
        .when(mockClient)
        .deleteFolder(any());

    // Act
    syncStrategy.deleteDirectory(
        GcsItemInfo.createFolder(rootFolder, 0L, 0L, 1L), /* recursive= */ true);

    // Assert
    assertThat(operations)
        .containsExactly(
            "deleteObject:root/",
            "deleteObject:root/sub/deep/file.txt",
            "deleteFolder:root/sub/deep/",
            "deleteFolder:root/sub/",
            "deleteFolder:root/")
        .inOrder();
  }

  @Test
  void deleteDirectory_recursiveServerSideAccessDenied_doesNotFallBack() throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    doThrow(new AccessDeniedException("gs://" + TEST_BUCKET + "/" + TEST_FOLDER + "/"))
        .when(mockClient)
        .deleteFolderRecursive(folderId);

    // Act & Assert
    assertThrows(
        AccessDeniedException.class,
        () ->
            strategy.deleteDirectory(
                GcsItemInfo.createFolder(folderId, 0L, 0L, 1L), /* recursive= */ true));
    verify(mockClient, never()).listObjectsPage(any(), any());
    verify(mockClient, never()).listFolders(any());
  }

  @Test
  void deleteDirectory_recursiveServerSidePlainIOException_doesNotFallBack() throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    doThrow(new IOException("Timed out waiting to delete folder recursively"))
        .when(mockClient)
        .deleteFolderRecursive(folderId);

    // Act & Assert
    assertThrows(
        IOException.class,
        () ->
            strategy.deleteDirectory(
                GcsItemInfo.createFolder(folderId, 0L, 0L, 1L), /* recursive= */ true));
    verify(mockClient, never()).listObjectsPage(any(), any());
    verify(mockClient, never()).listFolders(any());
  }

  @Test
  void deleteDirectory_recursiveServerSideInterruptedIOException_doesNotFallBack()
      throws IOException {
    // Arrange
    GcsItemId folderId = createFolderId(TEST_FOLDER + "/");
    doThrow(new InterruptedIOException("interrupted"))
        .when(mockClient)
        .deleteFolderRecursive(folderId);

    // Act & Assert
    assertThrows(
        InterruptedIOException.class,
        () ->
            strategy.deleteDirectory(
                GcsItemInfo.createFolder(folderId, 0L, 0L, 1L), /* recursive= */ true));
    verify(mockClient, never()).listObjectsPage(any(), any());
    verify(mockClient, never()).listFolders(any());
  }

  @Test
  void deleteDirectory_recursiveServerSideDisabled_runsClientSideDirectly() throws IOException {
    // Arrange
    HierarchicalNamespaceStrategyImpl clientSideStrategy =
        new HierarchicalNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setHnsServerSideRecursiveDeleteEnabled(false)
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId rootFolder = createFolderId("root/");
    GcsItemId childFolder = createFolderId("root/child/");
    when(mockClient.listObjectsPage(rootFolder, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());
    // Intentionally omit rootFolder from listFolders response to verify includeRootFolder adds it.
    when(mockClient.listFolders(rootFolder))
        .thenReturn(List.of(GcsItemInfo.createFolder(childFolder, 0L, 0L, 1L)));

    // Act
    clientSideStrategy.deleteDirectory(
        GcsItemInfo.createFolder(rootFolder, 0L, 0L, 1L), /* recursive= */ true);

    // Assert
    verify(mockClient, never()).deleteFolderRecursive(any());
    InOrder order = inOrder(mockClient);
    order.verify(mockClient).deleteFolder(childFolder);
    order.verify(mockClient).deleteFolder(rootFolder);
  }

  @Test
  void deleteDirectory_recursiveClientSidePhase1Failure_skipsPhase2FolderDeletion()
      throws IOException {
    // Arrange
    HierarchicalNamespaceStrategyImpl clientSideStrategy =
        new HierarchicalNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setHnsServerSideRecursiveDeleteEnabled(false)
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId rootFolder = createFolderId("root/");
    GcsItemId childFile =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("root/child.txt")
            .setContentGeneration(11L)
            .build();
    when(mockClient.listObjectsPage(rootFolder, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        GcsItemInfo.builder()
                            .setItemId(childFile)
                            .setSize(5L)
                            .setContentGeneration(11L)
                            .build()))
                .build());
    doThrow(new IOException("object delete failed")).when(mockClient).deleteObject(childFile);

    // Act
    IOException e =
        assertThrows(
            IOException.class,
            () ->
                clientSideStrategy.deleteDirectory(
                    GcsItemInfo.createFolder(rootFolder, 0L, 0L, 1L), /* recursive= */ true));

    // Assert
    assertThat(e).hasMessageThat().isEqualTo("object delete failed");
    verify(mockClient, never()).listFolders(any());
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteBucketContents_nonRecursiveEmpty_succeeds() throws IOException {
    // Arrange
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_HNS))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    // Act
    strategy.deleteBucketContents(bucketId, /* recursive= */ false);

    // Assert
    verify(mockClient).listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_HNS);
    verify(mockClient, never()).deleteFolder(any());
  }

  @Test
  void deleteBucketContents_nonRecursiveHasEmptyFolderPrefix_throwsDirectoryNotEmptyException()
      throws IOException {
    // Arrange
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.EMPTINESS_CHECK_HNS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixes(List.of(createFolderId("empty-folder/")))
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
  void deleteBucketContents_recursive_clearsClientSideNeverCallsDeleteFolderRecursive()
      throws IOException {
    // Arrange
    HierarchicalNamespaceStrategyImpl syncStrategy =
        new HierarchicalNamespaceStrategyImpl(
            mockClient,
            GcsFileSystemOptions.builder()
                .setHnsServerSideRecursiveDeleteEnabled(true)
                .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
                .build());
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    GcsItemId fileId =
        GcsItemId.builder()
            .setBucketName(TEST_BUCKET)
            .setObjectName("a/b/file.txt")
            .setContentGeneration(9L)
            .build();
    GcsItemId folderA = createFolderId("a/");
    GcsItemId folderAB = createFolderId("a/b/");
    when(mockClient.listObjectsPage(bucketId, NamespaceStrategy.DELETE_LIST_OPTIONS))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        GcsItemInfo.builder()
                            .setItemId(fileId)
                            .setSize(4L)
                            .setContentGeneration(9L)
                            .build()))
                .build());
    when(mockClient.listFolders(bucketId))
        .thenReturn(
            List.of(
                GcsItemInfo.createFolder(folderA, 0L, 0L, 1L),
                GcsItemInfo.createFolder(folderAB, 0L, 0L, 1L)));

    // Act
    syncStrategy.deleteBucketContents(bucketId, /* recursive= */ true);

    // Assert
    verify(mockClient, never()).deleteFolderRecursive(any());
    InOrder order = inOrder(mockClient);
    order.verify(mockClient).deleteObject(fileId);
    order.verify(mockClient).deleteFolder(folderAB);
    order.verify(mockClient).deleteFolder(folderA);
  }

  private static GcsItemId createGcsItemId(String objectName) {
    return GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemId createFolderId(String name) {
    return createGcsItemId(name);
  }
}
