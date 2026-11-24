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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

  private static final GcsListOptions RENAME_LIST_OPTIONS =
      GcsListOptions.LIST_RECURSIVE.toBuilder()
          .setFields(
              GcsListOptions.ListField.NAME,
              GcsListOptions.ListField.GENERATION,
              GcsListOptions.ListField.CRC32C,
              GcsListOptions.ListField.MD5HASH)
          .build();

  private static final Comparator<MovePair> MOVE_PAIR_COMPARATOR =
      Comparator.comparing(
          pair -> pair.srcItemInfo.getItemId().getObjectName().orElse(""),
          (a, b) ->
              a.length() == b.length() ? a.compareTo(b) : Integer.compare(a.length(), b.length()));

  private final GcsClient gcsClient;
  private final GcsFileSystemOptions fileSystemOptions;

  FlatNamespaceStrategyImpl(GcsClient gcsClient) {
    this(gcsClient, GcsFileSystemOptions.builder().build());
  }

  FlatNamespaceStrategyImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this.gcsClient = checkNotNull(gcsClient, "gcsClient must not be null");
    this.fileSystemOptions = checkNotNull(fileSystemOptions, "fileSystemOptions must not be null");
  }

  @Override
  public void createDirectory(GcsItemId id) throws IOException {
    UriUtil.validateDirectoryItemId(id);
    GcsItemId dirItemId = id.toDirectoryId();
    try {
      // Succeeds if a 0-byte marker already exists.
      gcsClient.createEmptyObject(dirItemId);
    } catch (FileAlreadyExistsException e) {
      // A non-empty object occupies "dir/", so the 0-byte marker can't be created. On HNS, writing
      // "dir/" also creates the native folder, so only flat buckets hit this; fail as
      // hadoop-connectors does.
      throw GcsExceptionUtil.createFileAlreadyExistsException(
          dirItemId, "A non-empty object already exists at the directory marker path", e);
    }
  }

  /**
   * Resolves directory metadata in a flat namespace bucket by listing the first object under the
   * directory prefix (e.g. {@code dir/}).
   *
   * <ul>
   *   <li>If the first listed object is the placeholder object itself ({@code dir/}), its real
   *       metadata (including generation) is returned.
   *   <li>If the first listed object is a child (e.g. {@code dir/obj.txt}), an inferred directory
   *       for {@code dir/} is returned (no generation, zero timestamps).
   * </ul>
   */
  @Override
  public GcsItemInfo getDirectoryInfo(GcsItemId id, boolean inferImplicitDirectories)
      throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    GcsItemId dirId = id.toDirectoryId();
    if (!inferImplicitDirectories) {
      // Do not list for implicit directories; only check for an explicit placeholder object.
      try {
        return gcsClient.getGcsItemInfo(dirId);
      } catch (FileNotFoundException e) {
        if (id.equals(dirId)) {
          throw e;
        }
        throw GcsExceptionUtil.createFileNotFoundException(id);
      }
    }
    // Returns the placeholder object "dir/" (if present) or the first child object under "dir/".
    Optional<GcsItemInfo> firstObject = gcsClient.listFirstObjectWithPrefix(dirId);
    if (firstObject.isEmpty()) {
      throw GcsExceptionUtil.createFileNotFoundException(id);
    }
    GcsItemInfo firstItemInfo = firstObject.get();
    // The placeholder object "dir/" sorts first under its own prefix, so if it exists it is listed.
    if (firstItemInfo.getItemId().getObjectName().equals(dirId.getObjectName())) {
      return firstItemInfo;
    }
    return GcsItemInfo.createInferredDirectory(dirId);
  }

  /**
   * Lists the directory with {@link GcsListOptions#LIST_DIRECTORY}. The directory exists if a page
   * returns a child or the directory's own placeholder object ({@code dir/}).
   */
  @Override
  public GcsListPage<GcsItemInfo> listDirectoryFirstPage(GcsItemId id) throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    return DirectoryListing.listFirstPage(gcsClient, id, GcsListOptions.LIST_DIRECTORY);
  }

  @Override
  public List<GcsItemInfo> completeDirectoryListing(
      GcsItemId id, GcsListPage<GcsItemInfo> firstPage) throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    DirectoryListing listing =
        DirectoryListing.listFromFirstPage(gcsClient, id, GcsListOptions.LIST_DIRECTORY, firstPage);
    if (!listing.isDirectoryFound() && id.isGcsObject()) {
      throw GcsExceptionUtil.createFileNotFoundException(id);
    }
    return listing.getChildren();
  }

  @Override
  public boolean shouldRepairImplicitParentDirectory() {
    return true;
  }

  @Override
  public void renameDirectory(GcsItemId srcDirId, GcsItemId dstDirId) throws IOException {
    checkNotNull(srcDirId, "srcDirId must not be null");
    checkNotNull(dstDirId, "dstDirId must not be null");
    checkArgument(srcDirId.isGcsObject(), "Expected GCS object srcDirId, got: %s", srcDirId);
    UriUtil.validateDirectoryItemId(dstDirId);
    checkArgument(
        srcDirId.getBucketName().equals(dstDirId.getBucketName()),
        "Source and destination directories must be in the same bucket: %s, %s",
        srcDirId,
        dstDirId);

    GcsItemId normalizedSrcDir = UriUtil.withoutContentGeneration(srcDirId).toDirectoryId();
    GcsItemId normalizedDstDir = UriUtil.withoutContentGeneration(dstDirId).toDirectoryId();
    String dstPrefix = normalizedDstDir.getObjectName().get();
    if (UriUtil.hasDotSegment(dstPrefix)) {
      throw new IOException(
          String.format(
              "Refusing to rename '%s' to '%s': destination contains '.' or '..' path segment",
              UriUtil.getStringPath(normalizedSrcDir), UriUtil.getStringPath(normalizedDstDir)));
    }

    GcsItemInfo selfPlaceholder = null;
    List<GcsItemInfo> listedChildren = new ArrayList<>();
    GcsListOptions listOptions = RENAME_LIST_OPTIONS;
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedIOException(
            String.format(
                "Interrupted while listing directory '%s' for rename",
                UriUtil.getStringPath(normalizedSrcDir)));
      }
      GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(normalizedSrcDir, listOptions);
      if (selfPlaceholder == null && page.getPrefixObject().isPresent()) {
        selfPlaceholder = page.getPrefixObject().get();
      }
      listedChildren.addAll(page.getItems());
      if (!page.hasNextPage()) {
        break;
      }
      listOptions =
          RENAME_LIST_OPTIONS.toBuilder()
              .setPageToken(page.getNextPageToken().orElseThrow())
              .build();
    }

    List<MovePair> nonMarkerMoves = new ArrayList<>();
    List<MovePair> markerMoves = new ArrayList<>();
    partitionAndValidateMoves(
        normalizedSrcDir,
        normalizedDstDir,
        selfPlaceholder,
        listedChildren,
        nonMarkerMoves,
        markerMoves);

    if (nonMarkerMoves.isEmpty() && markerMoves.isEmpty()) {
      return;
    }

    nonMarkerMoves.sort(MOVE_PAIR_COMPARATOR);
    markerMoves.sort(MOVE_PAIR_COMPARATOR);

    try (BatchExecutor batchExecutor =
        new BatchExecutor(fileSystemOptions.getGcsClientOptions().getBatchThreads())) {
      executeMovePhase(batchExecutor, nonMarkerMoves);
      executeMovePhase(batchExecutor, markerMoves);
    }
  }

  private void partitionAndValidateMoves(
      GcsItemId normalizedSrcDir,
      GcsItemId normalizedDstDir,
      GcsItemInfo selfPlaceholder,
      List<GcsItemInfo> listedChildren,
      List<MovePair> nonMarkerMoves,
      List<MovePair> markerMoves)
      throws IOException {
    String bucketName = normalizedSrcDir.getBucketName();
    String srcPrefix = normalizedSrcDir.getObjectName().get();
    String dstPrefix = normalizedDstDir.getObjectName().get();
    Optional<Pattern> markerFilePattern = fileSystemOptions.getMarkerFilePattern();

    boolean selfPlaceholderAdded = false;
    if (selfPlaceholder != null) {
      nonMarkerMoves.add(new MovePair(selfPlaceholder, normalizedDstDir));
      selfPlaceholderAdded = true;
    }

    for (GcsItemInfo child : listedChildren) {
      String srcObjectName = child.getItemId().getObjectName().orElse(null);
      if (srcObjectName == null || !srcObjectName.startsWith(srcPrefix)) {
        throw new IOException(
            String.format(
                "Refusing to rename '%s': object name '%s' is outside source '%s'",
                UriUtil.getStringPath(child.getItemId()),
                srcObjectName,
                UriUtil.getStringPath(normalizedSrcDir)));
      }
      if (srcObjectName.equals(srcPrefix)) {
        if (!selfPlaceholderAdded) {
          nonMarkerMoves.add(new MovePair(child, normalizedDstDir));
          selfPlaceholderAdded = true;
        }
        continue;
      }
      String relativePath = srcObjectName.substring(srcPrefix.length());
      UriUtil.validateRelativeChildPath(relativePath, child.getItemId(), normalizedDstDir);
      GcsItemId dstChildId =
          GcsItemId.builder()
              .setBucketName(bucketName)
              .setObjectName(dstPrefix + relativePath)
              .build();
      MovePair movePair = new MovePair(child, dstChildId);
      if (markerFilePattern.isPresent()
          && markerFilePattern.get().matcher(relativePath).matches()) {
        markerMoves.add(movePair);
      } else {
        nonMarkerMoves.add(movePair);
      }
    }
  }

  private void executeMovePhase(BatchExecutor batchExecutor, List<MovePair> moves)
      throws IOException {
    if (moves.isEmpty()) {
      return;
    }
    for (MovePair pair : moves) {
      batchExecutor.queue(
          () -> {
            GcsFileSystemImpl.moveObjectWithRetryRecovery(
                gcsClient, pair.srcItemInfo, pair.dstItemId);
            return null;
          },
          null);
    }
    batchExecutor.awaitRequestsCompletion();
  }

  private static final class MovePair {
    private final GcsItemInfo srcItemInfo;
    private final GcsItemId dstItemId;

    private MovePair(GcsItemInfo srcItemInfo, GcsItemId dstItemId) {
      this.srcItemInfo = srcItemInfo;
      this.dstItemId = dstItemId;
    }
  }
}
