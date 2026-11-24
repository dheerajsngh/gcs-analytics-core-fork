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

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HierarchicalNamespaceStrategyImpl implements NamespaceStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(HierarchicalNamespaceStrategyImpl.class);

  /**
   * Listing options for non-recursive HNS directory deletion checks: projects {@code SIZE} in
   * addition to {@code NAME} and {@code GENERATION} so a non-empty object literally named {@code
   * "dir/"} is distinguished from a 0-byte legacy directory placeholder.
   */
  private static final GcsListOptions HNS_NON_RECURSIVE_CHECK_OPTIONS =
      GcsListOptions.LIST_DIRECTORY_HNS.toBuilder()
          .setPageSize(2)
          .setFields(
              GcsListOptions.ListField.NAME,
              GcsListOptions.ListField.SIZE,
              GcsListOptions.ListField.GENERATION)
          .build();

  private final GcsClient gcsClient;
  private final GcsFileSystemOptions fileSystemOptions;

  HierarchicalNamespaceStrategyImpl(GcsClient gcsClient) {
    this(gcsClient, GcsFileSystemOptions.builder().build());
  }

  HierarchicalNamespaceStrategyImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this.gcsClient = checkNotNull(gcsClient, "gcsClient must not be null");
    this.fileSystemOptions = checkNotNull(fileSystemOptions, "fileSystemOptions must not be null");
  }

  @Override
  public void createDirectory(GcsItemId id) throws IOException {
    UriUtil.validateDirectoryItemId(id);
    String folderName = UriUtil.removeTrailingSlash(id.getObjectName().get());
    GcsItemId folderItemId =
        GcsItemId.builder().setBucketName(id.getBucketName()).setObjectName(folderName).build();
    try {
      gcsClient.createFolder(folderItemId, /* recursive= */ true);
    } catch (FileAlreadyExistsException e) {
      // The folder exists, also when a "dir/" object was written (GCS creates the folder then).
      LOG.debug("Folder already exists for item: {}", folderItemId, e);
    }
  }

  @Override
  public GcsItemInfo getDirectoryInfo(GcsItemId id, boolean inferImplicitDirectories)
      throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    // In HNS buckets, directories are always native folder resources; pass the ID as given so a
    // not-found error names the path the caller asked for, as in FlatNamespaceStrategyImpl.
    return gcsClient.getFolderInfo(id);
  }

  /**
   * Lists the folder with {@link GcsListOptions#LIST_DIRECTORY_HNS}, which also returns child
   * folders as prefixes. An empty folder lists nothing, so when nothing is listed for an object
   * path, {@link GcsClient#getFolderInfo} checks that the folder exists.
   */
  @Override
  public GcsListPage<GcsItemInfo> listDirectoryFirstPage(GcsItemId id) throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    return DirectoryListing.listFirstPage(gcsClient, id, GcsListOptions.LIST_DIRECTORY_HNS);
  }

  @Override
  public List<GcsItemInfo> completeDirectoryListing(
      GcsItemId id, GcsListPage<GcsItemInfo> firstPage) throws IOException {
    checkNotNull(id, "Item ID must not be null.");
    DirectoryListing listing =
        DirectoryListing.listFromFirstPage(
            gcsClient, id, GcsListOptions.LIST_DIRECTORY_HNS, firstPage);
    if (!listing.isDirectoryFound() && id.isGcsObject()) {
      // Throws FileNotFoundException naming id if the folder does not exist.
      gcsClient.getFolderInfo(id);
    }
    return listing.getChildren();
  }

  @Override
  public boolean shouldRepairImplicitParentDirectory() {
    return false;
  }

  @Override
  public void deleteDirectory(GcsItemInfo dirInfo, boolean recursive) throws IOException {
    checkNotNull(dirInfo, "dirInfo must not be null");
    GcsItemId dirId = dirInfo.getItemId().toDirectoryId();
    checkArgument(dirId.isGcsObject(), "Expected a directory itemId, but got: %s", dirId);
    if (!recursive) {
      deleteDirectoryNonRecursive(dirId);
      return;
    }

    if (fileSystemOptions.isHnsServerSideRecursiveDeleteEnabled()) {
      try {
        gcsClient.deleteFolderRecursive(dirId);
        return;
      } catch (DeleteFolderRecursiveRejectedException e) {
        // Log only the bucket at INFO: folder names may contain sensitive data.
        LOG.info(
            "Server rejected recursive folder delete in bucket {}; falling back to client-side"
                + " deletion",
            dirId.getBucketName());
        LOG.debug(
            "Falling back to client-side recursive deletion for {}",
            UriUtil.getStringPath(dirId),
            e);
        deleteHnsSubtreeClientSide(dirId, /* includeRootFolder= */ true);
        return;
      }
    }
    deleteHnsSubtreeClientSide(dirId, /* includeRootFolder= */ true);
  }

  @Override
  public void deleteBucketContents(GcsItemId bucketId, boolean recursive) throws IOException {
    checkNotNull(bucketId, "bucketId must not be null");
    checkArgument(bucketId.isBucket(), "Expected a bucket itemId, but got: %s", bucketId);
    if (!recursive) {
      if (!isBucketEmpty(bucketId)) {
        throw new DirectoryNotEmptyException(UriUtil.getStringPath(bucketId));
      }
      return;
    }
    // DeleteFolderRecursive does not accept bucket IDs; always clear HNS buckets client-side.
    deleteHnsSubtreeClientSide(bucketId, /* includeRootFolder= */ false);
  }

  private void deleteDirectoryNonRecursive(GcsItemId dirId) throws IOException {
    GcsItemId legacySelfPlaceholder = null;
    GcsListOptions listOptions = HNS_NON_RECURSIVE_CHECK_OPTIONS;
    while (true) {
      NamespaceStrategy.checkNotInterrupted(dirId);
      GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(dirId, listOptions);
      if (!page.getItems().isEmpty() || !page.getPrefixes().isEmpty()) {
        throw new DirectoryNotEmptyException(UriUtil.getStringPath(dirId));
      }
      if (page.getPrefixObject().isPresent()) {
        GcsItemInfo prefixObject = page.getPrefixObject().get();
        if (prefixObject.getSize() != 0L) {
          throw new DirectoryNotEmptyException(UriUtil.getStringPath(dirId));
        }
        legacySelfPlaceholder = NamespaceStrategy.itemIdWithPositiveGeneration(prefixObject);
      }
      if (!page.hasNextPage()) {
        break;
      }
      listOptions =
          HNS_NON_RECURSIVE_CHECK_OPTIONS.toBuilder()
              .setPageToken(page.getNextPageToken().orElseThrow())
              .build();
    }
    if (legacySelfPlaceholder != null) {
      NamespaceStrategy.checkNotInterrupted(dirId);
      gcsClient.deleteObject(legacySelfPlaceholder);
    }
    NamespaceStrategy.checkNotInterrupted(dirId);
    gcsClient.deleteFolder(dirId);
  }

  private boolean isBucketEmpty(GcsItemId bucketId) throws IOException {
    GcsListOptions listOptions = GcsListOptions.EMPTINESS_CHECK_HNS;
    while (true) {
      NamespaceStrategy.checkNotInterrupted(bucketId);
      GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(bucketId, listOptions);
      if (!page.getItems().isEmpty() || !page.getPrefixes().isEmpty()) {
        return false;
      }
      if (!page.hasNextPage()) {
        return true;
      }
      listOptions =
          GcsListOptions.EMPTINESS_CHECK_HNS.toBuilder()
              .setPageToken(page.getNextPageToken().orElseThrow())
              .build();
    }
  }

  private void deleteHnsSubtreeClientSide(GcsItemId prefixId, boolean includeRootFolder)
      throws IOException {
    int batchThreads = fileSystemOptions.getGcsClientOptions().getBatchThreads();
    try (BatchExecutor batchExecutor = new BatchExecutor(batchThreads)) {
      // Phase 1: stream all objects under prefixId (including any legacy "dir/" prefix object) and
      // delete them before removing native folders.
      GcsListOptions listOptions = NamespaceStrategy.DELETE_LIST_OPTIONS;
      while (true) {
        NamespaceStrategy.checkNotInterrupted(prefixId);
        GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(prefixId, listOptions);
        if (page.getPrefixObject().isPresent()) {
          GcsItemId prefixObjectId =
              NamespaceStrategy.itemIdWithPositiveGeneration(page.getPrefixObject().get());
          batchExecutor.queue(
              () -> {
                gcsClient.deleteObject(prefixObjectId);
                return null;
              },
              null);
        }
        for (GcsItemInfo item : page.getItems()) {
          GcsItemId objectId = NamespaceStrategy.itemIdWithPositiveGeneration(item);
          batchExecutor.queue(
              () -> {
                gcsClient.deleteObject(objectId);
                return null;
              },
              null);
        }
        if (!page.hasNextPage()) {
          break;
        }
        listOptions =
            NamespaceStrategy.DELETE_LIST_OPTIONS.toBuilder()
                .setPageToken(page.getNextPageToken().orElseThrow())
                .build();
      }
      batchExecutor.awaitRequestsCompletion();

      // Phase 2: list all native folders and delete them level-by-level from deepest to shallowest.
      NamespaceStrategy.checkNotInterrupted(prefixId);
      List<GcsItemInfo> folders = gcsClient.listFolders(prefixId);
      NavigableMap<Integer, List<GcsItemId>> foldersByDepth =
          new TreeMap<>(Collections.reverseOrder());
      Set<String> seenFolderPaths = new HashSet<>();
      for (GcsItemInfo folder : folders) {
        GcsItemId folderId = folder.getItemId().toDirectoryId();
        String folderPath = folderId.getObjectName().orElse("");
        if (!folderPath.isEmpty() && seenFolderPaths.add(folderPath)) {
          foldersByDepth
              .computeIfAbsent(UriUtil.directoryDepth(folderId), ignored -> new ArrayList<>())
              .add(folderId);
        }
      }
      if (includeRootFolder) {
        GcsItemId rootFolderId = prefixId.toDirectoryId();
        String rootFolderPath = rootFolderId.getObjectName().orElse("");
        if (!rootFolderPath.isEmpty() && seenFolderPaths.add(rootFolderPath)) {
          foldersByDepth
              .computeIfAbsent(UriUtil.directoryDepth(rootFolderId), ignored -> new ArrayList<>())
              .add(rootFolderId);
        }
      }
      for (List<GcsItemId> level : foldersByDepth.values()) {
        NamespaceStrategy.checkNotInterrupted(prefixId);
        for (GcsItemId folderId : level) {
          batchExecutor.queue(
              () -> {
                gcsClient.deleteFolder(folderId);
                return null;
              },
              null);
        }
        batchExecutor.awaitRequestsCompletion();
      }
    }
  }
}
