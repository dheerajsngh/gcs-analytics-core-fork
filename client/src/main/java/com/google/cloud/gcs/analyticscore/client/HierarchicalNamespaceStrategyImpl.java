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
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HierarchicalNamespaceStrategyImpl implements NamespaceStrategy {
  private static final Logger LOG =
      LoggerFactory.getLogger(HierarchicalNamespaceStrategyImpl.class);

  private final GcsClient gcsClient;

  HierarchicalNamespaceStrategyImpl(GcsClient gcsClient) {
    this(gcsClient, GcsFileSystemOptions.builder().build());
  }

  HierarchicalNamespaceStrategyImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this.gcsClient = checkNotNull(gcsClient, "gcsClient must not be null");
    checkNotNull(fileSystemOptions, "fileSystemOptions must not be null");
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
  public void renameDirectory(GcsItemId srcDirId, GcsItemId dstDirId) throws IOException {
    checkNotNull(srcDirId, "srcDirId must not be null");
    checkNotNull(dstDirId, "dstDirId must not be null");
    checkArgument(srcDirId.isGcsObject(), "Expected GCS object srcDirId, got: %s", srcDirId);
    UriUtil.validateDirectoryItemId(dstDirId);
    String srcObjectName = srcDirId.getObjectName().get();
    String dstObjectName = dstDirId.getObjectName().get();
    if (UriUtil.hasDotSegment(srcObjectName)) {
      throw new IOException(
          String.format(
              "Refusing to rename '%s' to '%s': source contains '.' or '..' path segment",
              UriUtil.getStringPath(srcDirId), UriUtil.getStringPath(dstDirId)));
    }
    if (UriUtil.hasDotSegment(dstObjectName)) {
      throw new IOException(
          String.format(
              "Refusing to rename '%s' to '%s': destination contains '.' or '..' path segment",
              UriUtil.getStringPath(srcDirId), UriUtil.getStringPath(dstDirId)));
    }
    gcsClient.renameFolder(
        UriUtil.withoutContentGeneration(srcDirId).toDirectoryId(),
        UriUtil.withoutContentGeneration(dstDirId).toDirectoryId());
  }
}
