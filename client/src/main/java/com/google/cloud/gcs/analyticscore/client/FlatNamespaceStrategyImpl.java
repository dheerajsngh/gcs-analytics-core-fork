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
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nullable;

final class FlatNamespaceStrategyImpl implements NamespaceStrategy {

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
  public void deleteDirectory(GcsItemInfo dirInfo, boolean recursive) throws IOException {
    checkNotNull(dirInfo, "dirInfo must not be null");
    GcsItemId dirId = dirInfo.getItemId().toDirectoryId();
    checkArgument(dirId.isGcsObject(), "Expected a directory itemId, but got: %s", dirId);
    if (!recursive) {
      if (!isDirectoryEmpty(dirId)) {
        throw new DirectoryNotEmptyException(UriUtil.getStringPath(dirId));
      }
      if (dirInfo.getItemType() != GcsItemInfo.ItemType.INFERRED_DIRECTORY) {
        NamespaceStrategy.checkNotInterrupted(dirId);
        gcsClient.deleteObject(NamespaceStrategy.itemIdWithPositiveGeneration(dirInfo));
      }
      return;
    }

    GcsItemId initialSelfPlaceholder =
        dirInfo.getItemType() == GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY
            ? NamespaceStrategy.itemIdWithPositiveGeneration(dirInfo)
            : null;
    deleteFlatSubtree(dirId, initialSelfPlaceholder);
  }

  @Override
  public void deleteBucketContents(GcsItemId bucketId, boolean recursive) throws IOException {
    checkNotNull(bucketId, "bucketId must not be null");
    checkArgument(bucketId.isBucket(), "Expected a bucket itemId, but got: %s", bucketId);
    if (!recursive) {
      if (!isDirectoryEmpty(bucketId)) {
        throw new DirectoryNotEmptyException(UriUtil.getStringPath(bucketId));
      }
      return;
    }
    deleteFlatSubtree(bucketId, /* initialSelfPlaceholder= */ null);
  }

  private boolean isDirectoryEmpty(GcsItemId dirOrBucketId) throws IOException {
    GcsItemId prefixId = dirOrBucketId.isBucket() ? dirOrBucketId : dirOrBucketId.toDirectoryId();
    GcsListOptions listOptions = GcsListOptions.EMPTINESS_CHECK_FLAT;
    while (true) {
      NamespaceStrategy.checkNotInterrupted(prefixId);
      GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(prefixId, listOptions);
      if (!page.getItems().isEmpty() || !page.getPrefixes().isEmpty()) {
        return false;
      }
      if (!page.hasNextPage()) {
        return true;
      }
      listOptions =
          GcsListOptions.EMPTINESS_CHECK_FLAT.toBuilder()
              .setPageToken(page.getNextPageToken().orElseThrow())
              .build();
    }
  }

  private void deleteFlatSubtree(GcsItemId prefixId, @Nullable GcsItemId initialSelfPlaceholder)
      throws IOException {
    GcsItemId selfPlaceholder = initialSelfPlaceholder;
    int batchThreads = fileSystemOptions.getGcsClientOptions().getBatchThreads();
    try (BatchExecutor batchExecutor = new BatchExecutor(batchThreads)) {
      List<GcsItemId> nestedDirPlaceholders = new ArrayList<>();
      GcsListOptions listOptions = NamespaceStrategy.DELETE_LIST_OPTIONS;
      while (true) {
        NamespaceStrategy.checkNotInterrupted(prefixId);
        GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(prefixId, listOptions);
        if (page.getPrefixObject().isPresent()) {
          selfPlaceholder =
              NamespaceStrategy.itemIdWithPositiveGeneration(page.getPrefixObject().get());
        }
        for (GcsItemInfo item : page.getItems()) {
          GcsItemId targetId = NamespaceStrategy.itemIdWithPositiveGeneration(item);
          if (targetId.isDirectory()) {
            nestedDirPlaceholders.add(targetId);
          } else {
            batchExecutor.queue(
                () -> {
                  gcsClient.deleteObject(targetId);
                  return null;
                },
                null);
          }
        }
        if (!page.hasNextPage()) {
          break;
        }
        listOptions =
            NamespaceStrategy.DELETE_LIST_OPTIONS.toBuilder()
                .setPageToken(page.getNextPageToken().orElseThrow())
                .build();
      }

      // Complete all non-directory object deletes before removing nested directory markers.
      batchExecutor.awaitRequestsCompletion();

      if (!nestedDirPlaceholders.isEmpty()) {
        NavigableMap<Integer, List<GcsItemId>> placeholdersByDepth =
            new TreeMap<>(Collections.reverseOrder());
        for (GcsItemId placeholderId : nestedDirPlaceholders) {
          placeholdersByDepth
              .computeIfAbsent(UriUtil.directoryDepth(placeholderId), ignored -> new ArrayList<>())
              .add(placeholderId);
        }
        for (List<GcsItemId> level : placeholdersByDepth.values()) {
          NamespaceStrategy.checkNotInterrupted(prefixId);
          for (GcsItemId placeholderId : level) {
            batchExecutor.queue(
                () -> {
                  gcsClient.deleteObject(placeholderId);
                  return null;
                },
                null);
          }
          batchExecutor.awaitRequestsCompletion();
        }
      }
    }

    // Delete the root target placeholder last so partial failures leave the directory observable.
    if (selfPlaceholder != null) {
      NamespaceStrategy.checkNotInterrupted(prefixId);
      gcsClient.deleteObject(selfPlaceholder);
    }
  }
}
