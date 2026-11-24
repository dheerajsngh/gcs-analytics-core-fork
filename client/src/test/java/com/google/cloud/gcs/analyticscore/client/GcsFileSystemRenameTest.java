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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.auth.AuthType;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Supplier;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit tests for {@link GcsFileSystemImpl#rename(GcsItemId, GcsItemId)} and helpers. */
@ExtendWith(MockitoExtension.class)
class GcsFileSystemRenameTest {

  private static final String BUCKET = "test-bucket";
  private static final String OTHER_BUCKET = "other-bucket";
  private static final byte[] CRC_A = new byte[] {1, 2, 3, 4};
  private static final byte[] CRC_B = new byte[] {5, 6, 7, 8};
  private static final byte[] MD5_A = new byte[] {9, 10, 11, 12};
  private static final byte[] MD5_B = new byte[] {13, 14, 15, 16};

  @Mock private GcsClient mockClient;

  private GcsFileSystemImpl flatFs;

  @BeforeEach
  void setUp() throws IOException {
    lenient()
        .when(mockClient.getBucketProperties(anyString()))
        .thenReturn(BucketProperties.create(false, false));
    lenient()
        .doAnswer(
            inv -> {
              throw GcsExceptionUtil.createFileNotFoundException(inv.getArgument(0));
            })
        .when(mockClient)
        .getGcsItemInfo(any());
    lenient().when(mockClient.listFirstObjectWithPrefix(any())).thenReturn(Optional.empty());
    lenient()
        .doAnswer(
            inv -> {
              throw GcsExceptionUtil.createFileNotFoundException(inv.getArgument(0));
            })
        .when(mockClient)
        .getFolderInfo(any());
    flatFs = new GcsFileSystemImpl(mockClient, defaultOptionsBuilder().build());
  }

  @AfterEach
  void tearDown() {
    if (flatFs != null) {
      flatFs.close();
    }
  }

  private static GcsFileSystemOptions.Builder defaultOptionsBuilder() {
    return GcsFileSystemOptions.builder()
        .setGcsClientOptions(GcsClientOptions.builder().setBatchThreads(0).build())
        .setGcsAuthOptions(GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build());
  }

  private static GcsItemId itemId(String objectName) {
    return GcsItemId.builder().setBucketName(BUCKET).setObjectName(objectName).build();
  }

  private static GcsItemId itemIdWithGen(String objectName, long generation) {
    return GcsItemId.builder()
        .setBucketName(BUCKET)
        .setObjectName(objectName)
        .setContentGeneration(generation)
        .build();
  }

  private static GcsItemId bucketId(String bucket) {
    return GcsItemId.builder().setBucketName(bucket).build();
  }

  private static GcsItemInfo objectInfo(String objectName, long generation) {
    return GcsItemInfo.builder()
        .setItemId(itemIdWithGen(objectName, generation))
        .setSize(16L)
        .setContentGeneration(generation)
        .setVerificationAttributes(VerificationAttributes.create(MD5_A, CRC_A))
        .setItemType(GcsItemInfo.ItemType.OBJECT)
        .build();
  }

  private static GcsItemInfo placeholderInfo(String dirName, long generation) {
    return GcsItemInfo.builder()
        .setItemId(itemIdWithGen(dirName, generation))
        .setSize(0L)
        .setContentGeneration(generation)
        .setVerificationAttributes(VerificationAttributes.create(MD5_A, CRC_A))
        .setItemType(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY)
        .build();
  }

  private static GcsItemInfo folderInfo(String dirName) {
    return GcsItemInfo.createFolder(itemId(dirName), 1L, 1L, 1L);
  }

  private void stubObject(GcsItemId id, GcsItemInfo info) throws IOException {
    doReturn(info).when(mockClient).getGcsItemInfo(id);
  }

  private void stubObjectFailure(GcsItemId id, Throwable failure) throws IOException {
    doThrow(failure).when(mockClient).getGcsItemInfo(id);
  }

  private void stubFolder(GcsItemId id, GcsItemInfo info) throws IOException {
    doReturn(info).when(mockClient).getFolderInfo(id);
  }

  // --- Input validation & URI overload ---

  @Test
  void rename_uriOverload_percentDecodesSpacesAndMovesObject() throws IOException {
    // Arrange
    URI srcUri = URI.create("gs://" + BUCKET + "/src%20file.txt");
    URI dstUri = URI.create("gs://" + BUCKET + "/dst%20file.txt");
    stubObject(itemId("src file.txt"), objectInfo("src file.txt", 11L));

    // Act
    flatFs.rename(srcUri, dstUri);

    // Assert
    verify(mockClient)
        .moveObject(itemIdWithGen("src file.txt", 11L), itemIdWithGen("dst file.txt", 0L));
  }

  @Test
  void rename_uriOverload_percentDecodesSpecialCharactersAndMovesObject() throws IOException {
    // Arrange
    GcsItemId srcId = itemId("dir/a b?c#d%25e.txt");
    GcsItemId dstId = itemId("dir/x y?z#1%25.txt");
    stubObject(srcId, objectInfo("dir/a b?c#d%25e.txt", 12L));
    when(mockClient.listFirstObjectWithPrefix(itemId("dir/")))
        .thenReturn(Optional.of(placeholderInfo("dir/", 2L)));

    // Act
    flatFs.rename(srcId.toUri(), dstId.toUri());

    // Assert
    verify(mockClient)
        .moveObject(
            itemIdWithGen("dir/a b?c#d%25e.txt", 12L), itemIdWithGen("dir/x y?z#1%25.txt", 0L));
  }

  @ParameterizedTest
  @ValueSource(strings = {"gs://test-bucket/a?b.txt", "gs://test-bucket/a#b.txt"})
  void rename_uriOverload_rawQueryOrFragment_throwsIllegalArgumentException(String invalidUri) {
    URI validUri = URI.create("gs://" + BUCKET + "/clean.txt");

    assertThrows(
        IllegalArgumentException.class, () -> flatFs.rename(URI.create(invalidUri), validUri));
    assertThrows(
        IllegalArgumentException.class, () -> flatFs.rename(validUri, URI.create(invalidUri)));
    verifyNoInteractions(mockClient);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "/dst.txt", "/dst/", "a//", "a//b", "a//b/"})
  void rename_malformedDestinationSlashes_throwsIllegalArgumentExceptionBeforeAnyRpc(
      String invalidDst) {
    assertThrows(
        IllegalArgumentException.class, () -> flatFs.rename(itemId("src.txt"), itemId(invalidDst)));
    assertThrows(
        IllegalArgumentException.class, () -> flatFs.rename(itemId("dir/"), itemId(invalidDst)));
    verifyNoInteractions(mockClient);
  }

  @Test
  void rename_inputWithContentGeneration_stripsInputGenerationsAndMovesLiveGeneration()
      throws IOException {
    stubObject(itemId("src.txt"), objectInfo("src.txt", 42L));

    flatFs.rename(itemIdWithGen("src.txt", 999L), itemIdWithGen("dst.txt", 888L));

    verify(mockClient).getGcsItemInfo(itemId("src.txt"));
    verify(mockClient).moveObject(itemIdWithGen("src.txt", 42L), itemIdWithGen("dst.txt", 0L));
  }

  @Test
  void rename_missingSrcWithContentGeneration_omitsGenerationFromFileNotFoundException() {
    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> flatFs.rename(itemIdWithGen("missing.txt", 999L), itemId("dst.txt")));

    assertThat(e).hasMessageThat().doesNotContain("#999");
    assertThat(e).hasMessageThat().contains("gs://" + BUCKET + "/missing.txt");
  }

  @Test
  void rename_nullArguments_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> flatFs.rename((GcsItemId) null, itemId("dst")));
    assertThrows(NullPointerException.class, () -> flatFs.rename(itemId("src"), (GcsItemId) null));
    assertThrows(
        NullPointerException.class,
        () -> flatFs.rename((URI) null, URI.create("gs://" + BUCKET + "/dst")));
    assertThrows(
        NullPointerException.class,
        () -> flatFs.rename(URI.create("gs://" + BUCKET + "/src"), (URI) null));
  }

  @Test
  void rename_rootSource_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> flatFs.rename(GcsItemId.ROOT, itemId("dst")));

    assertThat(e).hasMessageThat().contains("Root path cannot be renamed");
    verifyNoInteractions(mockClient);
  }

  @Test
  void rename_crossBucket_throwsUnsupportedOperationException() {
    GcsItemId src = itemId("a.txt");
    GcsItemId dst = GcsItemId.builder().setBucketName(OTHER_BUCKET).setObjectName("b.txt").build();

    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, () -> flatFs.rename(src, dst));

    assertThat(e).hasMessageThat().contains("Cross-bucket rename is not supported");
    verifyNoInteractions(mockClient);
  }

  @Test
  void rename_bucketSource_rejectsAllDestinationsWithUnsupportedOperationBeforeAnyRpc() {
    // Arrange
    GcsItemId srcBucket = bucketId(BUCKET);
    GcsItemId otherBucket = bucketId(OTHER_BUCKET);

    // Act & Assert
    assertThrows(UnsupportedOperationException.class, () -> flatFs.rename(srcBucket, otherBucket));
    assertThrows(
        UnsupportedOperationException.class, () -> flatFs.rename(srcBucket, GcsItemId.ROOT));
    assertThrows(UnsupportedOperationException.class, () -> flatFs.rename(srcBucket, srcBucket));
    assertThrows(
        UnsupportedOperationException.class, () -> flatFs.rename(srcBucket, itemId("sub/")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> flatFs.rename(srcBucket, itemId("existingFile")));
    verifyNoInteractions(mockClient);
  }

  @Test
  void rename_fileToRoot_throwsIOException() throws IOException {
    stubObject(itemId("a.txt"), objectInfo("a.txt", 1L));

    IOException e =
        assertThrows(IOException.class, () -> flatFs.rename(itemId("a.txt"), GcsItemId.ROOT));

    assertThat(e).hasMessageThat().isEqualTo("A file cannot be created in root.");
  }

  @Test
  void rename_directoryToRoot_throwsUnsupportedOperationException() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("dir/")))
        .thenReturn(Optional.of(placeholderInfo("dir/", 1L)));

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class,
            () -> flatFs.rename(itemId("dir/"), GcsItemId.ROOT));

    assertThat(e).hasMessageThat().contains("Cannot rename directory into root");
    verify(mockClient, never()).moveObject(any(), any());
    verify(mockClient, never()).renameFolder(any(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {".", "..", "a/./b", "a/../b", "a/.", "a/.."})
  void rename_dotSegmentInDestination_throwsIOExceptionBeforeAnyRpc(String invalidDst) {
    IOException e =
        assertThrows(IOException.class, () -> flatFs.rename(itemId("src.txt"), itemId(invalidDst)));

    assertThat(e).hasMessageThat().contains("destination contains '.' or '..' path segment");
    verifyNoInteractions(mockClient);
  }

  @Test
  void rename_dotSegmentInSrcLeafIntoExistingDir_throwsIOException() throws IOException {
    stubObject(itemId("a/.."), objectInfo("a/..", 1L));
    when(mockClient.listFirstObjectWithPrefix(itemId("target/")))
        .thenReturn(Optional.of(placeholderInfo("target/", 2L)));

    IOException e =
        assertThrows(IOException.class, () -> flatFs.rename(itemId("a/.."), itemId("target/")));

    assertThat(e).hasMessageThat().contains("invalid relative path");
    verify(mockClient, never()).moveObject(any(), any());
  }

  // --- Single file & target resolution semantics ---

  @Test
  void rename_singleFile_movesWithSrcGenAndDstGenZero() throws IOException {
    // Arrange
    stubObject(itemId("src.txt"), objectInfo("src.txt", 42L));

    // Act
    flatFs.rename(itemId("src.txt"), itemId("dst.txt"));

    // Assert
    verify(mockClient).moveObject(itemIdWithGen("src.txt", 42L), itemIdWithGen("dst.txt", 0L));
  }

  @ParameterizedTest
  @ValueSource(strings = {"target", "target/"})
  void rename_fileIntoExistingDirectory_resolvesChildAndMoves(String dstDirPath)
      throws IOException {
    // Arrange
    stubObject(itemId("src.txt"), objectInfo("src.txt", 7L));
    when(mockClient.listFirstObjectWithPrefix(itemId("target/")))
        .thenReturn(Optional.of(placeholderInfo("target/", 8L)));

    // Act
    flatFs.rename(itemId("src.txt"), itemId(dstDirPath));

    // Assert
    verify(mockClient)
        .moveObject(itemIdWithGen("src.txt", 7L), itemIdWithGen("target/src.txt", 0L));
  }

  @Test
  void rename_fileToNonExistentTrailingSlashDestination_throwsFileNotFoundException()
      throws IOException {
    // Arrange
    stubObject(itemId("src.txt"), objectInfo("src.txt", 1L));

    // Act & Assert
    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> flatFs.rename(itemId("src.txt"), itemId("missing/")));
    assertThat(e).hasMessageThat().contains("gs://" + BUCKET + "/missing/");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_fileToSelf_isNoOp() throws IOException {
    stubObject(itemId("a/f.txt"), objectInfo("a/f.txt", 3L));

    flatFs.rename(itemId("a/f.txt"), itemId("a/f.txt"));

    verify(mockClient, never()).moveObject(any(), any());
    verify(mockClient, never()).createEmptyObject(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"a", "a/"})
  void rename_fileIntoOwnParentDirectory_isNoOp(String parentDir) throws IOException {
    stubObject(itemId("a/f.txt"), objectInfo("a/f.txt", 3L));
    when(mockClient.listFirstObjectWithPrefix(itemId("a/")))
        .thenReturn(Optional.of(objectInfo("a/f.txt", 3L)));

    flatFs.rename(itemId("a/f.txt"), itemId(parentDir));

    verify(mockClient, never()).moveObject(any(), any());
    verify(mockClient, never()).createEmptyObject(any());
  }

  @Test
  void rename_topLevelFileIntoOwnBucket_isNoOpWithoutGetBucketInfo() throws IOException {
    stubObject(itemId("f.txt"), objectInfo("f.txt", 3L));

    flatFs.rename(itemId("f.txt"), bucketId(BUCKET));

    verify(mockClient, never()).moveObject(any(), any());
    verify(mockClient, never()).getBucketInfo(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"a", "a/"})
  void rename_directoryIntoOwnParentDirectory_isNoOp(String parentDir) throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("a/d/")))
        .thenReturn(Optional.of(placeholderInfo("a/d/", 5L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("a/")))
        .thenReturn(Optional.of(placeholderInfo("a/d/", 5L)));

    flatFs.rename(itemId("a/d/"), itemId(parentDir));

    verify(mockClient, never()).listObjectsPage(any(), any());
    verify(mockClient, never()).moveObject(any(), any());
    verify(mockClient, never()).renameFolder(any(), any());
  }

  @Test
  void rename_directoryToSelf_throwsIOException() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("d/")))
        .thenReturn(Optional.of(placeholderInfo("d/", 5L)));

    IOException e = assertThrows(IOException.class, () -> flatFs.rename(itemId("d"), itemId("d/")));

    assertThat(e).hasMessageThat().contains("Rename dir to self is forbidden");
  }

  @ParameterizedTest
  @ValueSource(strings = {"d/sub", "d/sub/"})
  void rename_directoryIntoSubdirectory_throwsIOException(String subdirDst) throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("d/")))
        .thenReturn(Optional.of(placeholderInfo("d/", 5L)));

    IOException e =
        assertThrows(IOException.class, () -> flatFs.rename(itemId("d/"), itemId(subdirDst)));

    assertThat(e).hasMessageThat().contains("Rename to subdir is forbidden");
  }

  @Test
  void rename_directoryToSiblingWithSharedPrefix_succeeds() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("dir/")))
        .thenReturn(Optional.of(placeholderInfo("dir/", 10L)));
    when(mockClient.listObjectsPage(eq(itemId("dir/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("dir/", 10L))
                .build());

    flatFs.rename(itemId("dir/"), itemId("dir2/"));

    verify(mockClient).moveObject(itemIdWithGen("dir/", 10L), itemIdWithGen("dir2/", 0L));
  }

  @Test
  void rename_directoryIntoExistingDirectory_resolvesChildDirectory() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("dir/")))
        .thenReturn(Optional.of(placeholderInfo("dir/", 10L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("target/")))
        .thenReturn(Optional.of(placeholderInfo("target/", 11L)));
    when(mockClient.listObjectsPage(eq(itemId("dir/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("dir/", 10L))
                .build());

    flatFs.rename(itemId("dir/"), itemId("target/"));

    verify(mockClient).moveObject(itemIdWithGen("dir/", 10L), itemIdWithGen("target/dir/", 0L));
  }

  @Test
  void rename_existingFileDestination_throwsFileAlreadyExistsException() throws IOException {
    stubObject(itemId("src.txt"), objectInfo("src.txt", 1L));
    stubObject(itemId("dst.txt"), objectInfo("dst.txt", 2L));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> flatFs.rename(itemId("src.txt"), itemId("dst.txt")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/dst.txt");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_directoryToExistingFileDestination_throwsFileAlreadyExistsException()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("src/")))
        .thenReturn(Optional.of(placeholderInfo("src/", 1L)));
    stubObject(itemId("existing-file"), objectInfo("existing-file", 2L));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> flatFs.rename(itemId("src/"), itemId("existing-file")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/existing-file");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_fileIntoExistingDirWithConflictingChildFile_throwsFileAlreadyExistsException()
      throws IOException {
    stubObject(itemId("src.txt"), objectInfo("src.txt", 1L));
    when(mockClient.listFirstObjectWithPrefix(itemId("dst/")))
        .thenReturn(Optional.of(placeholderInfo("dst/", 2L)));
    stubObject(itemId("dst/src.txt"), objectInfo("dst/src.txt", 3L));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> flatFs.rename(itemId("src.txt"), itemId("dst/")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/dst/src.txt");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_fileIntoExistingDirWithConflictingChildDir_throwsFileAlreadyExistsException()
      throws IOException {
    stubObject(itemId("item"), objectInfo("item", 1L));
    when(mockClient.listFirstObjectWithPrefix(itemId("dst/")))
        .thenReturn(Optional.of(placeholderInfo("dst/", 2L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("dst/item/")))
        .thenReturn(Optional.of(placeholderInfo("dst/item/", 3L)));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class, () -> flatFs.rename(itemId("item"), itemId("dst/")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/dst/item");
    assertThat(e.getOtherFile()).isEqualTo("gs://" + BUCKET + "/dst/item/");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_dirIntoExistingDirWithConflictingChildDir_throwsFileAlreadyExistsException()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("src/")))
        .thenReturn(Optional.of(placeholderInfo("src/", 1L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("dst/")))
        .thenReturn(Optional.of(placeholderInfo("dst/", 2L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("dst/src/")))
        .thenReturn(Optional.of(placeholderInfo("dst/src/", 3L)));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class, () -> flatFs.rename(itemId("src/"), itemId("dst/")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/dst/src/");
    verify(mockClient, never()).listObjectsPage(any(), any());
  }

  @Test
  void rename_missingSource_throwsFileNotFoundException() throws IOException {
    assertThrows(
        FileNotFoundException.class, () -> flatFs.rename(itemId("missing.txt"), itemId("dst.txt")));
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_missingDestinationParent_throwsFileNotFoundException() throws IOException {
    stubObject(itemId("src.txt"), objectInfo("src.txt", 1L));

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> flatFs.rename(itemId("src.txt"), itemId("missing-parent/dst.txt")));

    assertThat(e).hasMessageThat().contains("destination parent does not exist");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_fileShadowedDestinationParent_throwsFileNotFoundException() throws IOException {
    // Arrange: "parent" exists as a regular file, NOT as directory "parent/".
    stubObject(itemId("src.txt"), objectInfo("src.txt", 1L));
    lenient().doReturn(objectInfo("parent", 2L)).when(mockClient).getGcsItemInfo(itemId("parent"));

    // Act & Assert
    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> flatFs.rename(itemId("src.txt"), itemId("parent/dst.txt")));
    assertThat(e).hasMessageThat().contains("gs://" + BUCKET + "/parent/");
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_sameBucketTopLevelAndBucketDestWithObjectOnlyIam_neverCallsGetBucketInfo()
      throws IOException {
    // Arrange: deny bucket-level metadata reads (simulating object-only IAM / CAB).
    lenient()
        .when(mockClient.getBucketInfo(any()))
        .thenThrow(new AccessDeniedException("gs://" + BUCKET));
    stubObject(itemId("src.txt"), objectInfo("src.txt", 1L));
    stubObject(itemId("sub/nested.txt"), objectInfo("sub/nested.txt", 2L));

    // Act
    flatFs.rename(itemId("src.txt"), itemId("dst.txt"));
    flatFs.rename(itemId("sub/nested.txt"), bucketId(BUCKET));

    // Assert
    verify(mockClient).moveObject(itemIdWithGen("src.txt", 1L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient)
        .moveObject(itemIdWithGen("sub/nested.txt", 2L), itemIdWithGen("nested.txt", 0L));
    verify(mockClient, never()).getBucketInfo(any());
  }

  // --- Flat directory rename via GcsFileSystemImpl ---

  @Test
  void rename_flatDirectoryWithNestedSubdirsAndMarkerFiles_movesNonMarkersBeforeMarkers()
      throws IOException {
    // Arrange
    GcsFileSystemOptions options =
        defaultOptionsBuilder()
            .setMarkerFilePattern("_SUCCESS")
            .setAutoRepairImplicitDirectoriesEnabled(false)
            .build();
    try (GcsFileSystemImpl fs = new GcsFileSystemImpl(mockClient, options)) {
      when(mockClient.listFirstObjectWithPrefix(itemId("src/")))
          .thenReturn(Optional.of(placeholderInfo("src/", 1L)));
      when(mockClient.listObjectsPage(eq(itemId("src/")), any()))
          .thenReturn(
              GcsListPage.<GcsItemInfo>builder()
                  .setPrefixObject(placeholderInfo("src/", 1L))
                  .setItems(
                      List.of(
                          objectInfo("src/_SUCCESS", 2L),
                          placeholderInfo("src/sub/", 3L),
                          objectInfo("src/sub/part-0.parquet", 4L),
                          objectInfo("src/sub/_SUCCESS", 5L)))
                  .build());
      List<String> movedOrder = new ArrayList<>();
      doAnswer(
              inv -> {
                GcsItemId dst = inv.getArgument(1);
                movedOrder.add(dst.getObjectName().get());
                return null;
              })
          .when(mockClient)
          .moveObject(any(), any());

      // Act
      fs.rename(itemId("src"), itemId("dst"));

      // Assert
      assertThat(movedOrder)
          .containsExactly(
              "dst/", "dst/sub/", "dst/sub/part-0.parquet", "dst/_SUCCESS", "dst/sub/_SUCCESS")
          .inOrder();
    }
  }

  @Test
  void rename_flatEmptyDirectoryWithOnlyPlaceholder_movesPlaceholder() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("empty/")))
        .thenReturn(Optional.of(placeholderInfo("empty/", 9L)));
    when(mockClient.listObjectsPage(eq(itemId("empty/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("empty/", 9L))
                .build());

    flatFs.rename(itemId("empty/"), itemId("renamed/"));

    verify(mockClient).moveObject(itemIdWithGen("empty/", 9L), itemIdWithGen("renamed/", 0L));
  }

  @Test
  void rename_flatImplicitDirectory_movesChildrenWithoutPlaceholder() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("implicit/")))
        .thenReturn(Optional.of(objectInfo("implicit/a.txt", 10L)));
    when(mockClient.listObjectsPage(eq(itemId("implicit/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(
                        objectInfo("implicit/a.txt", 10L), objectInfo("implicit/sub/b.txt", 11L)))
                .build());

    flatFs.rename(itemId("implicit"), itemId("renamed"));

    verify(mockClient)
        .moveObject(itemIdWithGen("implicit/a.txt", 10L), itemIdWithGen("renamed/a.txt", 0L));
    verify(mockClient)
        .moveObject(
            itemIdWithGen("implicit/sub/b.txt", 11L), itemIdWithGen("renamed/sub/b.txt", 0L));
    verify(mockClient, never()).moveObject(eq(itemId("implicit/")), any());
  }

  @Test
  void rename_flatDirectoryWithInvalidDotSegmentChild_rejectsBeforeAnyMoveRpc() throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("src/")))
        .thenReturn(Optional.of(objectInfo("src/good.txt", 1L)));
    when(mockClient.listObjectsPage(eq(itemId("src/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setItems(
                    List.of(objectInfo("src/good.txt", 1L), objectInfo("src/sub/../bad.txt", 2L)))
                .build());

    IOException e =
        assertThrows(IOException.class, () -> flatFs.rename(itemId("src/"), itemId("dst/")));

    assertThat(e).hasMessageThat().contains("invalid relative path");
    verify(mockClient, never()).moveObject(any(), any());
  }

  // --- moveObjectWithRetryRecovery (404 & 412 lost-response recovery) ---

  @Test
  void moveObjectWithRetryRecovery_404WithMatchingCrc32c_recoversSuccessfully() throws IOException {
    GcsItemInfo srcInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("src.txt", 10L))
            .setSize(10L)
            .setContentGeneration(10L)
            .setVerificationAttributes(VerificationAttributes.create(null, CRC_A))
            .build();
    GcsItemInfo dstInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("dst.txt", 10L))
            .setSize(10L)
            .setContentGeneration(10L)
            .setVerificationAttributes(VerificationAttributes.create(null, CRC_A))
            .build();
    doThrow(GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L)))
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), dstInfo);

    GcsFileSystemImpl.moveObjectWithRetryRecovery(mockClient, srcInfo, itemId("dst.txt"));

    verify(mockClient).getGcsItemInfo(itemId("dst.txt"));
  }

  @Test
  void moveObjectWithRetryRecovery_404WithMatchingMd5_recoversSuccessfully() throws IOException {
    GcsItemInfo srcInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("src.txt", 10L))
            .setSize(10L)
            .setContentGeneration(10L)
            .setVerificationAttributes(VerificationAttributes.create(MD5_A, null))
            .build();
    GcsItemInfo dstInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("dst.txt", 10L))
            .setSize(10L)
            .setContentGeneration(10L)
            .setVerificationAttributes(VerificationAttributes.create(MD5_A, null))
            .build();
    doThrow(GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L)))
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), dstInfo);

    GcsFileSystemImpl.moveObjectWithRetryRecovery(mockClient, srcInfo, itemId("dst.txt"));

    verify(mockClient).getGcsItemInfo(itemId("dst.txt"));
  }

  @Test
  void moveObjectWithRetryRecovery_404WithMismatchedChecksum_rethrowsOriginal404()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    GcsItemInfo dstInfo =
        objectInfo("dst.txt", 10L).toBuilder()
            .setVerificationAttributes(VerificationAttributes.create(MD5_B, CRC_B))
            .build();
    FileNotFoundException original =
        GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L));
    doThrow(original)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), dstInfo);

    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(original);
  }

  @Test
  void moveObjectWithRetryRecovery_404SrcHasCrcAndMd5DstHasCrcOnly_rethrowsOriginal404()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    GcsItemInfo dstInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("dst.txt", 10L))
            .setSize(16L)
            .setContentGeneration(10L)
            .setVerificationAttributes(VerificationAttributes.create(null, CRC_A))
            .build();
    FileNotFoundException original =
        GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L));
    doThrow(original)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), dstInfo);

    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(original);
  }

  @Test
  void moveObjectWithRetryRecovery_404SrcHasCrcOnlyDstHasCrcAndMd5_rethrowsOriginal404()
      throws IOException {
    GcsItemInfo srcInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("src.txt", 10L))
            .setSize(16L)
            .setContentGeneration(10L)
            .setVerificationAttributes(VerificationAttributes.create(null, CRC_A))
            .build();
    GcsItemInfo dstInfo = objectInfo("dst.txt", 10L);
    FileNotFoundException original =
        GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L));
    doThrow(original)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), dstInfo);

    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(original);
  }

  @Test
  void moveObjectWithRetryRecovery_404WithoutSourceChecksum_rethrowsWithoutProbing()
      throws IOException {
    GcsItemInfo srcInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("src.txt", 10L))
            .setSize(10L)
            .setContentGeneration(10L)
            .build();
    FileNotFoundException original =
        GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L));
    doThrow(original)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));

    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(original);
    verify(mockClient, never()).getGcsItemInfo(any());
  }

  @Test
  void moveObjectWithRetryRecovery_404ProbeThrowsIOException_attachesSuppressedAndRethrows()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    FileNotFoundException original =
        GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L));
    IOException probeFailure = new IOException("transient probe failure");
    doThrow(original)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObjectFailure(itemId("dst.txt"), probeFailure);

    FileNotFoundException thrown =
        assertThrows(
            FileNotFoundException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(original);
    assertThat(thrown.getSuppressed()).asList().containsExactly(probeFailure);
  }

  @Test
  void moveObjectWithRetryRecovery_412MatchingChecksumAndMissingSource_recoversSuccessfully()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    IOException preconditionFailed =
        GcsExceptionUtil.translateMoveObjectException(
            new StorageException(412, "Precondition Failed"),
            itemIdWithGen("src.txt", 10L),
            itemIdWithGen("dst.txt", 0L));
    doThrow(preconditionFailed)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), objectInfo("dst.txt", 10L));
    // Default lenient stub throws FileNotFoundException for itemId("src.txt").

    GcsFileSystemImpl.moveObjectWithRetryRecovery(mockClient, srcInfo, itemId("dst.txt"));

    verify(mockClient).getGcsItemInfo(itemId("dst.txt"));
    verify(mockClient).getGcsItemInfo(itemId("src.txt"));
  }

  @Test
  void moveObjectWithRetryRecovery_412MatchingChecksumAndChangedSourceGen_recoversSuccessfully()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    IOException preconditionFailed =
        GcsExceptionUtil.translateMoveObjectException(
            new StorageException(412, "Precondition Failed"),
            itemIdWithGen("src.txt", 10L),
            itemIdWithGen("dst.txt", 0L));
    doThrow(preconditionFailed)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), objectInfo("dst.txt", 10L));
    stubObject(itemId("src.txt"), objectInfo("src.txt", 99L));

    GcsFileSystemImpl.moveObjectWithRetryRecovery(mockClient, srcInfo, itemId("dst.txt"));

    verify(mockClient).getGcsItemInfo(itemId("dst.txt"));
    verify(mockClient).getGcsItemInfo(itemId("src.txt"));
  }

  @Test
  void moveObjectWithRetryRecovery_412MatchingChecksumAndUnchangedSourceGen_rethrowsOriginal()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    IOException preconditionFailed =
        GcsExceptionUtil.translateMoveObjectException(
            new StorageException(412, "Precondition Failed"),
            itemIdWithGen("src.txt", 10L),
            itemIdWithGen("dst.txt", 0L));
    doThrow(preconditionFailed)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), objectInfo("dst.txt", 77L));
    stubObject(itemId("src.txt"), objectInfo("src.txt", 10L));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(preconditionFailed);
  }

  @Test
  void moveObjectWithRetryRecovery_412NoSourceGenerationAndLiveSourceExists_rethrowsOriginal()
      throws IOException {
    GcsItemInfo srcInfo =
        GcsItemInfo.builder()
            .setItemId(itemId("src.txt"))
            .setSize(10L)
            .setVerificationAttributes(VerificationAttributes.create(MD5_A, CRC_A))
            .build();
    IOException preconditionFailed =
        GcsExceptionUtil.translateMoveObjectException(
            new StorageException(412, "Precondition Failed"),
            itemId("src.txt"),
            itemIdWithGen("dst.txt", 0L));
    doThrow(preconditionFailed)
        .when(mockClient)
        .moveObject(itemId("src.txt"), itemIdWithGen("dst.txt", 0L));
    stubObject(itemId("dst.txt"), objectInfo("dst.txt", 77L));
    stubObject(itemId("src.txt"), objectInfo("src.txt", 10L));

    IOException thrown =
        assertThrows(
            FileAlreadyExistsException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(preconditionFailed);
  }

  @Test
  void moveObjectWithRetryRecovery_412WithoutSourceChecksum_rethrowsWithoutProbing()
      throws IOException {
    GcsItemInfo srcInfo =
        GcsItemInfo.builder()
            .setItemId(itemIdWithGen("src.txt", 10L))
            .setSize(10L)
            .setContentGeneration(10L)
            .build();
    IOException preconditionFailed =
        GcsExceptionUtil.translateMoveObjectException(
            new StorageException(412, "Precondition Failed"),
            itemIdWithGen("src.txt", 10L),
            itemIdWithGen("dst.txt", 0L));
    doThrow(preconditionFailed)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(preconditionFailed);
    verify(mockClient, never()).getGcsItemInfo(any());
  }

  @Test
  void moveObjectWithRetryRecovery_non412IOException_rethrowsWithoutProbing() throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    IOException serverError =
        GcsExceptionUtil.translateMoveObjectException(
            new StorageException(500, "Internal Error"),
            itemIdWithGen("src.txt", 10L),
            itemIdWithGen("dst.txt", 0L));
    doThrow(serverError)
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(serverError);
    verify(mockClient, never()).getGcsItemInfo(any());
  }

  @Test
  void moveObjectWithRetryRecovery_errorFromMoveOrProbe_propagatesErrorDirectly()
      throws IOException {
    GcsItemInfo srcInfo = objectInfo("src.txt", 10L);
    AssertionError fatalError = new AssertionError("fatal probe error");
    doThrow(GcsExceptionUtil.createFileNotFoundException(itemIdWithGen("src.txt", 10L)))
        .when(mockClient)
        .moveObject(itemIdWithGen("src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    stubObjectFailure(itemId("dst.txt"), fatalError);

    AssertionError thrown =
        assertThrows(
            AssertionError.class,
            () ->
                GcsFileSystemImpl.moveObjectWithRetryRecovery(
                    mockClient, srcInfo, itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(fatalError);
  }

  // --- HNS directory rename ---

  @Test
  void rename_hnsDirectory_usesRenameFolderAndSkipsImplicitParentRepair() throws IOException {
    // Arrange
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId("parent/src"), folderInfo("parent/src/"));
    stubFolder(itemId("parent/"), folderInfo("parent/"));

    // Act
    flatFs.rename(itemId("parent/src"), itemId("parent/dst"));

    // Assert
    verify(mockClient).renameFolder(itemId("parent/src/"), itemId("parent/dst/"));
    verify(mockClient, never()).createEmptyObject(any());
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_hnsDirectoryWithDotSegmentInSource_throwsIOExceptionWithoutRenameFolder()
      throws IOException {
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId("a/../src/"), folderInfo("a/../src/"));

    IOException e =
        assertThrows(IOException.class, () -> flatFs.rename(itemId("a/../src/"), itemId("dst/")));

    assertThat(e).hasMessageThat().contains("source contains '.' or '..' path segment");
    verify(mockClient, never()).renameFolder(any(), any());
  }

  @Test
  void rename_hnsFolderWithLeadingAndTrailingSpaces_renamesVerbatimViaUri() throws IOException {
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId(" dir/"), folderInfo(" dir/"));

    flatFs.rename(itemId(" dir/").toUri(), itemId("dir /").toUri());

    verify(mockClient).renameFolder(itemId(" dir/"), itemId("dir /"));
  }

  @Test
  void rename_hnsDirectoryIntoExistingFolder_resolvesChildAndCallsRenameFolder()
      throws IOException {
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId("src/"), folderInfo("src/"));
    stubFolder(itemId("target/"), folderInfo("target/"));

    flatFs.rename(itemId("src/"), itemId("target/"));

    verify(mockClient).renameFolder(itemId("src/"), itemId("target/src/"));
  }

  @Test
  void rename_hnsDirectoryIntoExistingFolderWithConflictingChild_throwsFileAlreadyExistsException()
      throws IOException {
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId("src/"), folderInfo("src/"));
    stubFolder(itemId("target/"), folderInfo("target/"));
    stubFolder(itemId("target/src/"), folderInfo("target/src/"));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> flatFs.rename(itemId("src/"), itemId("target/")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/target/src/");
    verify(mockClient, never()).renameFolder(any(), any());
  }

  // --- Flat implicit parent directory repair ---

  @Test
  void rename_flatNestedFileWithImplicitParent_repairsParentDirectory() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    // Explicit placeholder "parent/" is missing (default stub throws FileNotFoundException).

    flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

    verify(mockClient)
        .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient).createEmptyObject(itemId("parent/"));
  }

  @Test
  void rename_flatNestedFileWithExplicitParent_skipsParentRepair() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    stubObject(itemId("parent/"), placeholderInfo("parent/", 5L));

    flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

    verify(mockClient)
        .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient, never()).createEmptyObject(any());
  }

  @Test
  void rename_flatNestedFileWithAutoRepairDisabled_skipsParentRepair() throws IOException {
    GcsFileSystemOptions options =
        defaultOptionsBuilder().setAutoRepairImplicitDirectoriesEnabled(false).build();
    try (GcsFileSystemImpl fs = new GcsFileSystemImpl(mockClient, options)) {
      stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));

      fs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

      verify(mockClient)
          .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
      verify(mockClient, never()).getGcsItemInfo(itemId("parent/"));
      verify(mockClient, never()).createEmptyObject(any());
    }
  }

  @Test
  void rename_flatImplicitParentProbeThrowsIOException_renameStillSucceeds() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    stubObjectFailure(itemId("parent/"), new IOException("Transient probe error"));

    flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

    verify(mockClient)
        .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient, never()).createEmptyObject(any());
  }

  @Test
  void rename_flatImplicitParentProbeThrowsInterruptedIOException_restoresInterruptFlagAndSucceeds()
      throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    stubObjectFailure(itemId("parent/"), new InterruptedIOException("Probe interrupted"));

    try {
      flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
    verify(mockClient)
        .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient, never()).createEmptyObject(any());
  }

  @Test
  void rename_flatImplicitParentCreateEmptyObjectThrowsIOException_renameStillSucceeds()
      throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    doThrow(new IOException("Repair write failed"))
        .when(mockClient)
        .createEmptyObject(itemId("parent/"));

    flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

    verify(mockClient)
        .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient).createEmptyObject(itemId("parent/"));
  }

  @Test
  void rename_flatImplicitParentProbeThrowsError_propagatesError() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    AssertionError fatalError = new AssertionError("fatal probe error");
    stubObjectFailure(itemId("parent/"), fatalError);

    AssertionError thrown =
        assertThrows(
            AssertionError.class, () -> flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(fatalError);
  }

  @Test
  void rename_flatImplicitParentCreateEmptyObjectThrowsError_propagatesError() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    AssertionError fatalError = new AssertionError("fatal repair error");
    doThrow(fatalError).when(mockClient).createEmptyObject(itemId("parent/"));

    AssertionError thrown =
        assertThrows(
            AssertionError.class, () -> flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt")));

    assertThat(thrown).isSameInstanceAs(fatalError);
  }

  @Test
  void rename_moveFails_doesNotRepairImplicitParent() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    doThrow(new IOException("Move failed")).when(mockClient).moveObject(any(), any());

    assertThrows(
        IOException.class, () -> flatFs.rename(itemId("parent/src.txt"), itemId("dst.txt")));

    verify(mockClient, never()).createEmptyObject(any());
  }

  @Test
  void rename_directoryToNonExistentTrailingSlashPathWithExistingFile_throwsFileAlreadyExists()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("srcDir/")))
        .thenReturn(Optional.of(placeholderInfo("srcDir/", 10L)));
    stubObject(itemId("foo"), objectInfo("foo", 20L));

    FileAlreadyExistsException e =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> flatFs.rename(itemId("srcDir/"), itemId("foo/")));

    assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/foo/");
    assertThat(e.getOtherFile()).isEqualTo("gs://" + BUCKET + "/foo");
    verify(mockClient, never()).moveObject(any(), any());
    verify(mockClient, never()).renameFolder(any(), any());
  }

  @Test
  void rename_hnsDirectoryToNonExistentTrailingSlashPathWithExistingFile_throwsFileAlreadyExists()
      throws IOException {
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId("srcDir/"), folderInfo("srcDir/"));
    stubObject(itemId("foo"), objectInfo("foo", 20L));

    try (GcsFileSystemImpl hnsFs =
        new GcsFileSystemImpl(mockClient, defaultOptionsBuilder().build())) {
      FileAlreadyExistsException e =
          assertThrows(
              FileAlreadyExistsException.class,
              () -> hnsFs.rename(itemId("srcDir/"), itemId("foo/")));

      assertThat(e.getFile()).isEqualTo("gs://" + BUCKET + "/foo/");
      assertThat(e.getOtherFile()).isEqualTo("gs://" + BUCKET + "/foo");
      verify(mockClient, never()).renameFolder(any(), any());
    }
  }

  @Test
  void rename_directoryToExistingTrailingSlashDirWithCoexistingFile_movesIntoDirectory()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("srcDir/")))
        .thenReturn(Optional.of(placeholderInfo("srcDir/", 10L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("foo/")))
        .thenReturn(Optional.of(placeholderInfo("foo/", 20L)));
    lenient().doReturn(objectInfo("foo", 21L)).when(mockClient).getGcsItemInfo(itemId("foo"));
    when(mockClient.listObjectsPage(eq(itemId("srcDir/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("srcDir/", 10L))
                .build());

    flatFs.rename(itemId("srcDir/"), itemId("foo/"));

    verify(mockClient).moveObject(itemIdWithGen("srcDir/", 10L), itemIdWithGen("foo/srcDir/", 0L));
  }

  @Test
  void rename_fileToNonExistentTrailingSlashPathWithExistingFile_throwsFileNotFound()
      throws IOException {
    stubObject(itemId("foo"), objectInfo("foo", 20L));

    // Renaming a file to "foo/" when only file "foo" exists (not directory "foo/") must fail with
    // FileNotFoundException rather than treating it as a no-op self-rename.
    assertThrows(FileNotFoundException.class, () -> flatFs.rename(itemId("foo"), itemId("foo/")));
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_leadingSlashSourceObject_doesNotCreateSpuriousSlashParentPlaceholder()
      throws IOException {
    stubObject(itemId("/file.txt"), objectInfo("/file.txt", 30L));

    flatFs.rename(itemId("/file.txt"), itemId("dst.txt"));

    verify(mockClient).moveObject(itemIdWithGen("/file.txt", 30L), itemIdWithGen("dst.txt", 0L));
    verify(mockClient, never()).createEmptyObject(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/a/file.txt", "a//file.txt", "staging/../file.txt", "a/./file.txt"})
  void rename_flatMalformedOrDotSegmentSourceParent_skipsImplicitParentRepair(String srcPath)
      throws IOException {
    stubObject(itemId(srcPath), objectInfo(srcPath, 31L));

    flatFs.rename(itemId(srcPath), itemId("clean.txt"));

    verify(mockClient).moveObject(itemIdWithGen(srcPath, 31L), itemIdWithGen("clean.txt", 0L));
    verify(mockClient, never()).createEmptyObject(any());
  }

  // --- Prefix-scoped CAB 403 on trailing-slash non-directory probes ---

  @Test
  void rename_flatDirectoryToNonExistentTrailingSlashDst_accessDeniedOnNonDirProbe_succeeds()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("staging/")))
        .thenReturn(Optional.of(placeholderInfo("staging/", 10L)));
    when(mockClient.listObjectsPage(eq(itemId("staging/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("staging/", 10L))
                .build());
    stubObjectFailure(itemId("final"), new AccessDeniedException("gs://" + BUCKET + "/final"));

    flatFs.rename(itemId("staging/"), itemId("final/"));

    verify(mockClient).moveObject(itemIdWithGen("staging/", 10L), itemIdWithGen("final/", 0L));
  }

  @Test
  void rename_hnsDirectoryToNonExistentTrailingSlashDst_accessDeniedOnNonDirProbe_succeeds()
      throws IOException {
    when(mockClient.getBucketProperties(BUCKET)).thenReturn(BucketProperties.create(true, false));
    stubFolder(itemId("staging/"), folderInfo("staging/"));
    stubObjectFailure(itemId("final"), new AccessDeniedException("gs://" + BUCKET + "/final"));

    flatFs.rename(itemId("staging/"), itemId("final/"));

    verify(mockClient).renameFolder(itemId("staging/"), itemId("final/"));
  }

  @Test
  void rename_directoryToTrailingSlashDst_accessDeniedOnDirProbe_propagatesAccessDenied()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("staging/")))
        .thenReturn(Optional.of(placeholderInfo("staging/", 10L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("final/")))
        .thenThrow(new AccessDeniedException("gs://" + BUCKET + "/final/"));

    assertThrows(
        AccessDeniedException.class, () -> flatFs.rename(itemId("staging/"), itemId("final/")));
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_directoryIntoBucketRoot_accessDeniedOnStrippedNonDirProbe_succeeds()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("sub/final/")))
        .thenReturn(Optional.of(placeholderInfo("sub/final/", 10L)));
    when(mockClient.listObjectsPage(eq(itemId("sub/final/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("sub/final/", 10L))
                .build());
    stubObjectFailure(itemId("final"), new AccessDeniedException("gs://" + BUCKET + "/final"));

    flatFs.rename(itemId("sub/final/"), bucketId(BUCKET));

    verify(mockClient).moveObject(itemIdWithGen("sub/final/", 10L), itemIdWithGen("final/", 0L));
  }

  @Test
  void rename_directoryIntoExistingDir_accessDeniedOnStrippedResolvedNonDirProbe_succeeds()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("src/")))
        .thenReturn(Optional.of(placeholderInfo("src/", 10L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("target/")))
        .thenReturn(Optional.of(placeholderInfo("target/", 11L)));
    when(mockClient.listObjectsPage(eq(itemId("src/")), any()))
        .thenReturn(
            GcsListPage.<GcsItemInfo>builder()
                .setPrefixObject(placeholderInfo("src/", 10L))
                .build());
    stubObjectFailure(
        itemId("target/src"), new AccessDeniedException("gs://" + BUCKET + "/target/src"));

    flatFs.rename(itemId("src/"), itemId("target/"));

    verify(mockClient).moveObject(itemIdWithGen("src/", 10L), itemIdWithGen("target/src/", 0L));
  }

  @Test
  void rename_directoryIntoExistingDir_accessDeniedOnResolvedDirProbe_propagatesAccessDenied()
      throws IOException {
    when(mockClient.listFirstObjectWithPrefix(itemId("src/")))
        .thenReturn(Optional.of(placeholderInfo("src/", 10L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("target/")))
        .thenReturn(Optional.of(placeholderInfo("target/", 11L)));
    when(mockClient.listFirstObjectWithPrefix(itemId("target/src/")))
        .thenThrow(new AccessDeniedException("gs://" + BUCKET + "/target/src/"));

    assertThrows(
        AccessDeniedException.class, () -> flatFs.rename(itemId("src/"), itemId("target/")));
    verify(mockClient, never()).moveObject(any(), any());
  }

  @Test
  void rename_statusExecutorRejectsParentProbe_stillCompletesRenameBestEffort() throws IOException {
    stubObject(itemId("parent/src.txt"), objectInfo("parent/src.txt", 10L));
    ExecutorService realExecutor = Executors.newSingleThreadExecutor();
    AtomicInteger submitCount = new AtomicInteger();
    ExecutorService rejectingOnParentProbeExecutor =
        new AbstractExecutorService() {
          @Override
          public void shutdown() {
            realExecutor.shutdown();
          }

          @Override
          public List<Runnable> shutdownNow() {
            return realExecutor.shutdownNow();
          }

          @Override
          public boolean isShutdown() {
            return realExecutor.isShutdown();
          }

          @Override
          public boolean isTerminated() {
            return realExecutor.isTerminated();
          }

          @Override
          public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return realExecutor.awaitTermination(timeout, unit);
          }

          @Override
          public void execute(Runnable command) {
            // Submissions: dstObjFuture (#1), dstDirFuture (#2), getFileInfo(src) dir check (#3),
            // then srcParentProbeFuture (#4).
            if (submitCount.incrementAndGet() >= 4) {
              throw new RejectedExecutionException("shutting down");
            }
            realExecutor.execute(command);
          }
        };
    try (GcsFileSystemImpl customFs =
        new GcsFileSystemImpl(mockClient, defaultOptionsBuilder().build()) {
          @Override
          Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
            return () -> rejectingOnParentProbeExecutor;
          }
        }) {
      customFs.rename(itemId("parent/src.txt"), itemId("dst.txt"));

      verify(mockClient)
          .moveObject(itemIdWithGen("parent/src.txt", 10L), itemIdWithGen("dst.txt", 0L));
      verify(mockClient, never()).createEmptyObject(any());
    } finally {
      realExecutor.shutdownNow();
    }
  }

  // --- Concurrency & deadlock freedom ---

  @Test
  void rename_concurrentRenamesWithSingleThreadStatusPool_doesNotDeadlock() throws Exception {
    // Arrange: 1-thread status pool ensures any nested statusExecutor submission would deadlock.
    ExecutorService singleThreadStatusPool = Executors.newSingleThreadExecutor();
    int callerCount = 4;
    ExecutorService callerPool = Executors.newFixedThreadPool(callerCount);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch readyLatch = new CountDownLatch(callerCount);
    AtomicInteger movedCount = new AtomicInteger();

    for (int i = 0; i < callerCount; i++) {
      String srcName = "dir" + i + "/file" + i + ".txt";
      stubObject(itemId(srcName), objectInfo(srcName, i + 1L));
      when(mockClient.listFirstObjectWithPrefix(itemId("dst" + i + "/")))
          .thenReturn(Optional.of(placeholderInfo("dst" + i + "/", i + 100L)));
    }
    doAnswer(
            inv -> {
              movedCount.incrementAndGet();
              return null;
            })
        .when(mockClient)
        .moveObject(any(), any());

    try (GcsFileSystemImpl singleThreadedFs =
        new GcsFileSystemImpl(mockClient, defaultOptionsBuilder().build()) {
          @Override
          Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
            return () -> singleThreadStatusPool;
          }
        }) {
      // Act
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < callerCount; i++) {
        int idx = i;
        futures.add(
            callerPool.submit(
                () -> {
                  readyLatch.countDown();
                  startGate.await();
                  singleThreadedFs.rename(
                      itemId("dir" + idx + "/file" + idx + ".txt"),
                      itemId("dst" + idx + "/moved" + idx + ".txt"));
                  return null;
                }));
      }
      assertThat(readyLatch.await(5, TimeUnit.SECONDS)).isTrue();
      startGate.countDown();

      // Assert
      for (Future<Void> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
      assertThat(movedCount.get()).isEqualTo(callerCount);
    } finally {
      callerPool.shutdownNow();
      singleThreadStatusPool.shutdownNow();
    }
  }

  @Test
  void rename_metadataLookupParallelDisabled_succeedsWithLazyExecutor() throws IOException {
    GcsFileSystemOptions options =
        defaultOptionsBuilder().setMetadataLookupParallelEnabled(false).build();
    try (GcsFileSystemImpl lazyFs = new GcsFileSystemImpl(mockClient, options)) {
      stubObject(itemId("src.txt"), objectInfo("src.txt", 12L));

      lazyFs.rename(itemId("src.txt"), itemId("dst.txt"));

      verify(mockClient).moveObject(itemIdWithGen("src.txt", 12L), itemIdWithGen("dst.txt", 0L));
    }
  }

  @Test
  void publicSignatures_doNotExposeGuavaTypes() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsFileSystem.class, GcsFileSystemOptions.class, GcsFileSystemOptions.Builder.class);
  }
}
