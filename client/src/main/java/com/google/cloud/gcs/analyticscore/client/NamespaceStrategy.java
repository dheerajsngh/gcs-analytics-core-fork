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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Optional;

/**
 * Strategy interface for handling directory operations across different namespace models (Flat vs.
 * HNS).
 *
 * <p>Methods for directory operations will be added in follow-up PRs. These methods will include:
 *
 * <ul>
 *   <li>{@code boolean isDirectoryEmpty(GcsItemId id) throws IOException;}
 *   <li>{@code void renameDirectory(GcsItemId src, GcsItemId dst) throws IOException;}
 *   <li>{@code java.util.List<GcsItemInfo> listRecursive(GcsItemId id) throws IOException;}
 * </ul>
 */
interface NamespaceStrategy {

  /**
   * Creates a directory at the given {@link GcsItemId}, normalizing the trailing {@code "/"} as
   * required by the underlying namespace model. Succeeds when the directory's own resource exists
   * afterwards, whether this call created it or it already existed:
   *
   * <ul>
   *   <li><b>Flat namespace ({@link FlatNamespaceStrategyImpl})</b>: the resource is a 0-byte
   *       directory marker object ({@code "dir/"}). Succeeds if a 0-byte marker already exists, and
   *       throws {@link FileAlreadyExistsException} if a non-empty ({@code size > 0}) object
   *       already exists at the marker key, because the marker can't be created there.
   *   <li><b>Hierarchical namespace ({@link HierarchicalNamespaceStrategyImpl})</b>: the resource
   *       is a native folder ({@code "dir"}), created recursively via the Storage Control API.
   *       Succeeds if the folder already exists, including when a non-empty object named {@code
   *       "dir/"} exists, since GCS automatically creates the native folder when that object is
   *       written.
   * </ul>
   *
   * @param id the directory identifier (with or without a trailing {@code "/"})
   * @throws IllegalArgumentException if {@code id} does not represent a GCS object, or if its
   *     object name starts with {@code "/"} or contains consecutive {@code "/"} characters
   * @throws FileAlreadyExistsException if a non-empty object already exists at the directory marker
   *     path on a flat-namespace bucket
   * @throws FileNotFoundException if the target bucket does not exist
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws IOException if an I/O error occurs during directory creation
   */
  void createDirectory(GcsItemId id) throws IOException;

  /**
   * Retrieves directory metadata for the given {@link GcsItemId}, inferring implicit directories on
   * flat-namespace buckets.
   *
   * @param id the item identifier for the directory
   * @return the {@link GcsItemInfo} representing the directory
   * @throws FileNotFoundException if the directory does not exist
   * @throws IOException if an I/O error occurs
   */
  default GcsItemInfo getDirectoryInfo(GcsItemId id) throws IOException {
    return getDirectoryInfo(id, /* inferImplicitDirectories= */ true);
  }

  /**
   * Minimal field projection used when recursively listing objects for deletion. Requests only
   * object names and content generations so {@link GcsClient#deleteObject(GcsItemId)} can enforce
   * generation-match preconditions while minimizing listing payload size.
   */
  GcsListOptions DELETE_LIST_OPTIONS =
      GcsListOptions.LIST_RECURSIVE.toBuilder()
          .setFields(GcsListOptions.ListField.NAME, GcsListOptions.ListField.GENERATION)
          .build();

  /**
   * Retrieves directory metadata for the given {@link GcsItemId}.
   *
   * @param id the item identifier for the directory
   * @param inferImplicitDirectories if {@code false}, flat-namespace buckets only check for an
   *     explicit directory placeholder object ({@code dir/}) without prefix-listing child objects;
   *     HNS buckets always query the native folder resource
   * @return the {@link GcsItemInfo} representing the directory
   * @throws FileNotFoundException if the directory does not exist
   * @throws IOException if an I/O error occurs
   */
  GcsItemInfo getDirectoryInfo(GcsItemId id, boolean inferImplicitDirectories) throws IOException;

  /**
   * Lists the immediate children of the directory {@code id.toDirectoryId()}, fetching every page
   * of results. A bucket {@code id} lists the top level of the bucket.
   *
   * <p>The children are objects, including child placeholder objects such as {@code dir/sub/}, and
   * subdirectories without such an object, which are returned as inferred directories. Those are
   * implicit directories and, on hierarchical namespace buckets, folders. The directory's own
   * placeholder object is not returned, and each object name is returned once: an object wins over
   * an inferred directory with the same name.
   *
   * @param id the directory (with or without a trailing {@code "/"}) or bucket to list
   * @return an unmodifiable list of the immediate children in the order they were listed; empty for
   *     an existing empty directory or bucket
   * @throws FileNotFoundException if {@code id} is an object path and no directory exists there, or
   *     if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to list the directory
   * @throws InterruptedIOException if the calling thread is interrupted before a page of the
   *     listing is requested; its interrupt status stays set
   * @throws IOException if an I/O error occurs while listing
   */
  default List<GcsItemInfo> listDirectory(GcsItemId id) throws IOException {
    return completeDirectoryListing(id, listDirectoryFirstPage(id));
  }

  /**
   * Lists only the first page of {@code id.toDirectoryId()}. Callers that overlap listing with an
   * object lookup run this single-RPC call on the status pool and finish the listing on the calling
   * thread via {@link #completeDirectoryListing}.
   */
  GcsListPage<GcsItemInfo> listDirectoryFirstPage(GcsItemId id) throws IOException;

  /**
   * Finishes a directory listing started by {@link #listDirectoryFirstPage}, fetching any remaining
   * pages and verifying directory existence when nothing was listed.
   */
  List<GcsItemInfo> completeDirectoryListing(GcsItemId id, GcsListPage<GcsItemInfo> firstPage)
      throws IOException;

  /**
   * Returns {@code true} if deleting an object or directory under this namespace model may require
   * creating a 0-byte placeholder marker for its immediate parent directory so the parent remains
   * observable.
   */
  boolean shouldRepairImplicitParentDirectory();

  /**
   * Deletes the directory described by {@code dirInfo}.
   *
   * @param dirInfo metadata for the existing directory to delete
   * @param recursive if {@code true}, deletes all nested objects and subdirectories; if {@code
   *     false}, throws {@link DirectoryNotEmptyException} when the directory is not empty
   * @throws DirectoryNotEmptyException if {@code recursive} is {@code false} and the directory is
   *     not empty
   * @throws IOException if an I/O error occurs
   */
  void deleteDirectory(GcsItemInfo dirInfo, boolean recursive) throws IOException;

  /**
   * Verifies emptiness of (when {@code recursive} is {@code false}) or deletes all contents inside
   * (when {@code recursive} is {@code true}) the bucket identified by {@code bucketId}, without
   * deleting the bucket resource itself.
   *
   * @param bucketId the bucket identifier
   * @param recursive if {@code true}, deletes all objects and folders in the bucket; if {@code
   *     false}, throws {@link DirectoryNotEmptyException} when the bucket is not empty
   * @throws DirectoryNotEmptyException if {@code recursive} is {@code false} and the bucket is not
   *     empty
   * @throws IOException if an I/O error occurs
   */
  void deleteBucketContents(GcsItemId bucketId, boolean recursive) throws IOException;

  /**
   * Throws {@link InterruptedIOException} (preserving the thread's interrupt status) if the current
   * thread has been interrupted while deleting {@code itemId}.
   */
  static void checkNotInterrupted(GcsItemId itemId) throws InterruptedIOException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException(
          "Interrupted while deleting: " + UriUtil.getStringPath(itemId));
    }
  }

  /**
   * Returns an identifier for {@code itemInfo} carrying a positive content generation if known from
   * either {@link GcsItemInfo#getItemId()} or {@link GcsItemInfo#getContentGeneration()}, or
   * without any generation otherwise (stripping non-positive generations).
   */
  static GcsItemId itemIdWithPositiveGeneration(GcsItemInfo itemInfo) {
    GcsItemId itemId = itemInfo.getItemId();
    if (itemId.getContentGeneration().filter(gen -> gen > 0L).isPresent()) {
      return itemId;
    }
    Optional<Long> infoGen = itemInfo.getContentGeneration().filter(gen -> gen > 0L);
    if (infoGen.isPresent()) {
      GcsItemId.Builder builder = GcsItemId.builder().setBucketName(itemId.getBucketName());
      itemId.getObjectName().ifPresent(builder::setObjectName);
      builder.setContentGeneration(infoGen.get());
      return builder.build();
    }
    return UriUtil.withoutContentGeneration(itemId);
  }
}
