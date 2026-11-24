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
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  void renameDirectory_emptyDirWithOnlyPlaceholder_movesPlaceholderInSingleRpc()
      throws IOException {
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    GcsItemInfo placeholder = createGcsItemInfoWithGen("src/", 10L);
    GcsListOptions expectedListOptions =
        GcsListOptions.LIST_RECURSIVE.toBuilder()
            .setFields(
                GcsListOptions.ListField.NAME,
                GcsListOptions.ListField.GENERATION,
                GcsListOptions.ListField.CRC32C,
                GcsListOptions.ListField.MD5HASH)
            .build();
    when(mockClient.listObjectsPage(srcDir, expectedListOptions))
        .thenReturn(GcsListPage.<GcsItemInfo>builder().setPrefixObject(placeholder).build());

    strategy.renameDirectory(srcDir, dstDir);

    verify(mockClient)
        .moveObject(createGcsItemIdWithGen("src/", 10L), createGcsItemIdWithGen("dst/", 0L));
  }

  @Test
  void renameDirectory_multiPageWithMarkerFiles_movesNonMarkersBeforeMarkersInDeterministicOrder()
      throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setMarkerFilePattern("_SUCCESS|\\.done")
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
            .build();
    FlatNamespaceStrategyImpl configuredStrategy =
        new FlatNamespaceStrategyImpl(mockClient, options);
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    GcsListOptions firstPageOptions =
        GcsListOptions.LIST_RECURSIVE.toBuilder()
            .setFields(
                GcsListOptions.ListField.NAME,
                GcsListOptions.ListField.GENERATION,
                GcsListOptions.ListField.CRC32C,
                GcsListOptions.ListField.MD5HASH)
            .build();
    GcsListOptions secondPageOptions = firstPageOptions.toBuilder().setPageToken("page-2").build();

    GcsItemInfo selfPlaceholder = createGcsItemInfoWithGen("src/", 1L);
    GcsItemInfo nestedFile = createGcsItemInfoWithGen("src/sub/data.parquet", 2L);
    GcsItemInfo topMarker = createGcsItemInfoWithGen("src/_SUCCESS", 3L);
    GcsItemInfo topFile = createGcsItemInfoWithGen("src/a.txt", 4L);
    GcsItemInfo nestedMarker = createGcsItemInfoWithGen("src/sub/.done", 5L);

    when(mockClient.listObjectsPage(srcDir, firstPageOptions))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(selfPlaceholder)
                .setItems(List.of(nestedFile, topMarker))
                .setNextPageToken("page-2")
                .build());
    when(mockClient.listObjectsPage(srcDir, secondPageOptions))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder().setItems(List.of(topFile, nestedMarker)).build());

    List<String> movedOrder = new ArrayList<>();
    doAnswer(
            invocation -> {
              GcsItemId dst = invocation.getArgument(1);
              movedOrder.add(dst.getObjectName().get());
              return null;
            })
        .when(mockClient)
        .moveObject(any(), any());

    configuredStrategy.renameDirectory(srcDir, dstDir);

    assertThat(movedOrder)
        .containsExactly(
            "dst/", "dst/a.txt", "dst/sub/data.parquet", "dst/_SUCCESS", "dst/sub/.done")
        .inOrder();
  }

  @Test
  void renameDirectory_phase1Failure_doesNotMoveMarkerFiles() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setMarkerFilePattern("_SUCCESS")
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
            .build();
    FlatNamespaceStrategyImpl configuredStrategy =
        new FlatNamespaceStrategyImpl(mockClient, options);
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        createGcsItemInfoWithGen("src/data.txt", 1L),
                        createGcsItemInfoWithGen("src/_SUCCESS", 2L)))
                .build());
    doThrow(new IOException("Move failed"))
        .when(mockClient)
        .moveObject(
            createGcsItemIdWithGen("src/data.txt", 1L), createGcsItemIdWithGen("dst/data.txt", 0L));

    IOException e =
        assertThrows(IOException.class, () -> configuredStrategy.renameDirectory(srcDir, dstDir));

    assertThat(e).hasMessageThat().isEqualTo("Move failed");
    verify(mockClient, never())
        .moveObject(
            createGcsItemIdWithGen("src/_SUCCESS", 2L), createGcsItemIdWithGen("dst/_SUCCESS", 0L));
  }

  @Test
  void renameDirectory_invalidDotSegmentChild_failsBeforeAnyMoveRpc() throws IOException {
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        createGcsItemInfoWithGen("src/valid.txt", 1L),
                        createGcsItemInfoWithGen("src/sub/../evil.txt", 2L)))
                .build());

    IOException e = assertThrows(IOException.class, () -> strategy.renameDirectory(srcDir, dstDir));

    assertThat(e).hasMessageThat().contains("invalid relative path");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void renameDirectory_consecutiveSlashesInChild_failsBeforeAnyMoveRpc() throws IOException {
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        createGcsItemInfoWithGen("src/valid.txt", 1L),
                        createGcsItemInfoWithGen("src/a//b.txt", 2L)))
                .build());

    IOException e = assertThrows(IOException.class, () -> strategy.renameDirectory(srcDir, dstDir));

    assertThat(e).hasMessageThat().contains("invalid relative path");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "/dst/", "a//", "a//b/"})
  void renameDirectory_malformedDestinationSlashes_throwsIllegalArgumentException(
      String invalidDst) {
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId(invalidDst);

    assertThrows(IllegalArgumentException.class, () -> strategy.renameDirectory(srcDir, dstDir));
    verifyNoInteractions(mockClient);
  }

  @Test
  void renameDirectory_interruptedWhileListing_throwsInterruptedIOExceptionBeforeAnyMove()
      throws IOException {
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    Thread.currentThread().interrupt();
    try {
      assertThrows(InterruptedIOException.class, () -> strategy.renameDirectory(srcDir, dstDir));
    } finally {
      Thread.interrupted();
    }
    verifyNoInteractions(mockClient);
  }

  @Test
  void renameDirectory_multiThreadedBatchExecutor_completesPhase1BeforePhase2() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setMarkerFilePattern("_SUCCESS")
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(4).build())
            .build();
    FlatNamespaceStrategyImpl multiThreadedStrategy =
        new FlatNamespaceStrategyImpl(mockClient, options);
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        createGcsItemInfoWithGen("src/part-0.parquet", 1L),
                        createGcsItemInfoWithGen("src/part-1.parquet", 2L),
                        createGcsItemInfoWithGen("src/part-2.parquet", 3L),
                        createGcsItemInfoWithGen("src/_SUCCESS", 4L)))
                .build());
    AtomicInteger phase1Completed = new AtomicInteger();
    AtomicBoolean markerRanBeforePhase1Finished = new AtomicBoolean(false);
    List<String> moved = Collections.synchronizedList(new ArrayList<>());
    doAnswer(
            inv -> {
              GcsItemId dst = inv.getArgument(1);
              String name = dst.getObjectName().get();
              if (name.endsWith("_SUCCESS")) {
                if (phase1Completed.get() < 3) {
                  markerRanBeforePhase1Finished.set(true);
                }
              } else {
                phase1Completed.incrementAndGet();
              }
              moved.add(name);
              return null;
            })
        .when(mockClient)
        .moveObject(any(), any());

    multiThreadedStrategy.renameDirectory(srcDir, dstDir);

    assertThat(markerRanBeforePhase1Finished.get()).isFalse();
    assertThat(moved)
        .containsExactly(
            "dst/part-0.parquet", "dst/part-1.parquet", "dst/part-2.parquet", "dst/_SUCCESS");
    assertThat(moved.get(moved.size() - 1)).isEqualTo("dst/_SUCCESS");
  }

  @Test
  void renameDirectory_multiThreadedBatchExecutor_phase1FailureSkipsPhase2() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setMarkerFilePattern("_SUCCESS")
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(4).build())
            .build();
    FlatNamespaceStrategyImpl multiThreadedStrategy =
        new FlatNamespaceStrategyImpl(mockClient, options);
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        createGcsItemInfoWithGen("src/part-0.parquet", 1L),
                        createGcsItemInfoWithGen("src/part-1.parquet", 2L),
                        createGcsItemInfoWithGen("src/_SUCCESS", 3L)))
                .build());
    doThrow(new IOException("Phase 1 failure"))
        .when(mockClient)
        .moveObject(
            createGcsItemIdWithGen("src/part-1.parquet", 2L),
            createGcsItemIdWithGen("dst/part-1.parquet", 0L));

    IOException e =
        assertThrows(
            IOException.class, () -> multiThreadedStrategy.renameDirectory(srcDir, dstDir));

    assertThat(e).hasMessageThat().isEqualTo("Phase 1 failure");
    verify(mockClient, never())
        .moveObject(
            createGcsItemIdWithGen("src/_SUCCESS", 3L), createGcsItemIdWithGen("dst/_SUCCESS", 0L));
  }

  @Test
  void renameDirectory_multiThreadedBatchExecutor_propagatesError() throws IOException {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(2).build())
            .build();
    FlatNamespaceStrategyImpl multiThreadedStrategy =
        new FlatNamespaceStrategyImpl(mockClient, options);
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(createGcsItemInfoWithGen("src/part-0.parquet", 1L)))
                .build());
    AssertionError fatalError = new AssertionError("worker fatal error");
    doThrow(fatalError).when(mockClient).moveObject(any(), any());

    AssertionError thrown =
        assertThrows(
            AssertionError.class, () -> multiThreadedStrategy.renameDirectory(srcDir, dstDir));

    assertThat(thrown).isSameInstanceAs(fatalError);
  }

  @Test
  void renameDirectory_multiThreadedBatchExecutor_interruptedWhileAwaiting_throwsInterruptedIO()
      throws Exception {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(2).build())
            .build();
    FlatNamespaceStrategyImpl multiThreadedStrategy =
        new FlatNamespaceStrategyImpl(mockClient, options);
    GcsItemId srcDir = createGcsItemId("src/");
    GcsItemId dstDir = createGcsItemId("dst/");
    when(mockClient.listObjectsPage(eq(srcDir), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(List.of(createGcsItemInfoWithGen("src/part-0.parquet", 1L)))
                .build());
    CountDownLatch moveStarted = new CountDownLatch(1);
    CountDownLatch releaseMove = new CountDownLatch(1);
    doAnswer(
            inv -> {
              moveStarted.countDown();
              releaseMove.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(mockClient)
        .moveObject(any(), any());

    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              try {
                if (moveStarted.await(5, TimeUnit.SECONDS)) {
                  callerThread.interrupt();
                }
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
              }
            });
    interrupter.start();
    try {
      assertThrows(
          InterruptedIOException.class,
          () -> multiThreadedStrategy.renameDirectory(srcDir, dstDir));
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      releaseMove.countDown();
      interrupter.join(5_000);
    }
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

  private static GcsItemInfo createGcsItemInfoWithGen(String objectName, long generation) {
    return GcsItemInfo.builder()
        .setItemId(createGcsItemIdWithGen(objectName, generation))
        .setSize(10L)
        .setContentGeneration(generation)
        .build();
  }
}
