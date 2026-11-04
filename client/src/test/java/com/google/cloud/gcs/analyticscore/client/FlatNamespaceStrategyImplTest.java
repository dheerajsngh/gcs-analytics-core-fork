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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.StorageException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileAlreadyExistsException;
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
  void listDirectory_objectsAndPrefixes_returnsObjectsAndInferredDirectories() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemInfo file = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    GcsItemInfo childPlaceholder = createPlaceholderInfo(TEST_DIR + "/sub/");
    GcsItemId implicitDirId = createGcsItemId(TEST_DIR + "/implicit/");
    when(mockClient.listObjectsPage(dirId.toDirectoryId(), GcsListOptions.LIST_DIRECTORY))
        .thenReturn(
            listPage(List.of(file, childPlaceholder), List.of(implicitDirId), Optional.empty()));

    List<GcsItemInfo> children = strategy.listDirectory(dirId);

    assertThat(children)
        .containsExactly(
            file, childPlaceholder, GcsItemInfo.createInferredDirectory(implicitDirId));
  }

  @Test
  void listDirectory_multiplePages_followsPageTokensAndDeduplicatesAcrossPages()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    GcsItemInfo file = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    GcsItemInfo firstPlaceholder = createPlaceholderInfo(TEST_DIR + "/first/");
    GcsItemInfo laterPlaceholder = createPlaceholderInfo(TEST_DIR + "/later/");
    GcsItemId firstPrefix = createGcsItemId(TEST_DIR + "/first/");
    GcsItemId laterPrefix = createGcsItemId(TEST_DIR + "/later/");
    GcsItemId implicitDirId = createGcsItemId(TEST_DIR + "/implicit/");
    // "first/" is listed as an object before its prefix, and "later/" as a prefix before its
    // object.
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(
            listPage(
                List.of(file, firstPlaceholder), List.of(laterPrefix), Optional.of("token-1")));
    when(mockClient.listObjectsPage(dirId, listDirectoryOptions("token-1")))
        .thenReturn(
            listPage(List.of(laterPlaceholder), List.of(implicitDirId), Optional.of("token-2")));
    when(mockClient.listObjectsPage(dirId, listDirectoryOptions("token-2")))
        .thenReturn(listPage(List.of(), List.of(firstPrefix, implicitDirId), Optional.empty()));

    List<GcsItemInfo> children = strategy.listDirectory(dirId);

    assertThat(children)
        .containsExactly(
            file,
            firstPlaceholder,
            laterPlaceholder,
            GcsItemInfo.createInferredDirectory(implicitDirId));
    verify(mockClient).listObjectsPage(dirId, listDirectoryOptions("token-1"));
    verify(mockClient).listObjectsPage(dirId, listDirectoryOptions("token-2"));
    verify(mockClient, times(3)).listObjectsPage(eq(dirId), any());
  }

  @Test
  void listDirectory_placeholderListedAsPrefixObject_excludesPlaceholder() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    GcsItemInfo file = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    when(mockClient.listObjectsPage(dirId.toDirectoryId(), GcsListOptions.LIST_DIRECTORY))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(file))
                .setPrefixObject(createPlaceholderInfo(TEST_DIR + "/"))
                .build());

    List<GcsItemInfo> children = strategy.listDirectory(dirId);

    assertThat(children).containsExactly(file);
  }

  @Test
  void listDirectory_emptyDirectoryWithPlaceholderOnEarlierPage_returnsEmptyList()
      throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(createPlaceholderInfo(TEST_DIR + "/"))
                .setNextPageToken("token-1")
                .build());
    when(mockClient.listObjectsPage(dirId, listDirectoryOptions("token-1")))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    List<GcsItemInfo> children = strategy.listDirectory(dirId);

    assertThat(children).isEmpty();
  }

  @Test
  void listDirectory_interruptedBetweenPages_throwsInterruptedIOException() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR + "/");
    GcsItemInfo file = createGcsItemInfo(TEST_DIR + "/file.txt", 100L);
    when(mockClient.listObjectsPage(dirId, GcsListOptions.LIST_DIRECTORY))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return listPage(List.of(file), List.of(), Optional.of("token-1"));
            });

    try {
      InterruptedIOException e =
          assertThrows(InterruptedIOException.class, () -> strategy.listDirectory(dirId));

      assertThat(e).hasMessageThat().isEqualTo("Interrupted while listing " + dirId);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      verify(mockClient, times(1)).listObjectsPage(any(), any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void listDirectory_missingDirectory_throwsFileNotFoundExceptionNamingItemId() throws IOException {
    GcsItemId dirId = createGcsItemId(TEST_DIR);
    when(mockClient.listObjectsPage(dirId.toDirectoryId(), GcsListOptions.LIST_DIRECTORY))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.listDirectory(dirId));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "Location does not exist or generation not found: gs://"
                + TEST_BUCKET
                + "/"
                + TEST_DIR);
    verify(mockClient, never()).getFolderInfo(any());
  }

  @Test
  void listDirectory_emptyBucket_returnsEmptyList() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.LIST_DIRECTORY))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().build());

    List<GcsItemInfo> children = strategy.listDirectory(bucketId);

    assertThat(children).isEmpty();
  }

  @Test
  void listDirectory_missingBucket_throwsFileNotFoundException() throws IOException {
    GcsItemId bucketId = GcsItemId.builder().setBucketName(TEST_BUCKET).build();
    FileNotFoundException bucketNotFound =
        GcsExceptionUtil.createBucketNotFoundException(TEST_BUCKET);
    when(mockClient.listObjectsPage(bucketId, GcsListOptions.LIST_DIRECTORY))
        .thenThrow(bucketNotFound);

    FileNotFoundException e =
        assertThrows(FileNotFoundException.class, () -> strategy.listDirectory(bucketId));

    assertThat(e).isSameInstanceAs(bucketNotFound);
  }

  private static GcsItemId createGcsItemId(String objectName) {
    return GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemInfo createGcsItemInfo(String objectName, long size) {
    return GcsItemInfo.builder().setItemId(createGcsItemId(objectName)).setSize(size).build();
  }

  private static GcsItemInfo createPlaceholderInfo(String objectName) {
    return GcsItemInfo.createPlaceholderDirectory(createGcsItemId(objectName));
  }

  private static GcsListOptions listDirectoryOptions(String pageToken) {
    return GcsListOptions.LIST_DIRECTORY.toBuilder().setPageToken(pageToken).build();
  }

  private static GcsListPage<GcsItemInfo> listPage(
      List<GcsItemInfo> items, List<GcsItemId> prefixes, Optional<String> nextPageToken) {
    return GcsListPage.<GcsItemInfo>builder()
        .setItems(items)
        .setPrefixes(prefixes)
        .setNextPageToken(nextPageToken)
        .build();
  }
}
