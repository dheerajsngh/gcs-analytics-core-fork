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
import java.nio.file.AccessDeniedException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The immediate children of a directory, collected from every page of {@link
 * GcsClient#listObjectsPage}. Shared by the {@link NamespaceStrategy} implementations.
 */
final class DirectoryListing {

  private final List<GcsItemInfo> children;
  private final boolean directoryFound;

  private DirectoryListing(List<GcsItemInfo> children, boolean directoryFound) {
    this.children = children;
    this.directoryFound = directoryFound;
  }

  /**
   * Lists every page of the immediate children of {@code id.toDirectoryId()}. A bucket {@code id}
   * lists the top level of the bucket.
   *
   * <p>Listed objects are kept as they are, and listed prefixes become inferred directories. Each
   * object name is kept once across all pages: an object wins over a prefix with the same name.
   *
   * @param gcsClient the client that lists each page
   * @param id the directory (with or without a trailing {@code "/"}) or bucket to list
   * @param options the options of the first page request; later requests add the page token
   * @return the listed children, and whether the listing proves that the directory exists
   * @throws IllegalArgumentException if {@code id} has an empty bucket name, or if {@code options}
   *     return the directory's own placeholder object as a listed item ({@link
   *     GcsListOptions#isIncludePrefixObject()})
   * @throws FileNotFoundException if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to list the directory
   * @throws InterruptedIOException if the calling thread is interrupted when a page is about to be
   *     requested; its interrupt status stays set
   * @throws IOException if a page cannot be listed
   */
  static DirectoryListing list(GcsClient gcsClient, GcsItemId id, GcsListOptions options)
      throws IOException {
    checkNotNull(gcsClient, "gcsClient must not be null");
    checkNotNull(id, "id must not be null");
    checkNotNull(options, "options must not be null");
    checkArgument(
        !options.isIncludePrefixObject(),
        "options must not list the directory's own placeholder object as an item");

    GcsItemId dirId = id.toDirectoryId();
    // Keyed by object name, so that an object listed on any page replaces a same-named prefix.
    Map<String, GcsItemInfo> childrenByName = new LinkedHashMap<>();
    boolean placeholderFound = false;
    GcsListOptions pageOptions = options;
    while (true) {
      // The caller may cancel a listing that runs in the background; stop fetching pages then.
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedIOException(
            "Interrupted while listing " + UriUtil.getStringPath(dirId));
      }
      GcsListPage<GcsItemInfo> page = gcsClient.listObjectsPage(dirId, pageOptions);
      placeholderFound |= page.getPrefixObject().isPresent();
      for (GcsItemInfo item : page.getItems()) {
        childrenByName.put(item.getItemId().getObjectName().orElse(""), item);
      }
      for (GcsItemId prefix : page.getPrefixes()) {
        childrenByName.putIfAbsent(
            prefix.getObjectName().orElse(""), GcsItemInfo.createInferredDirectory(prefix));
      }
      if (!page.hasNextPage()) {
        break;
      }
      pageOptions = options.toBuilder().setPageToken(page.getNextPageToken().get()).build();
    }
    return new DirectoryListing(
        List.copyOf(childrenByName.values()), placeholderFound || !childrenByName.isEmpty());
  }

  /** Returns the immediate children as an unmodifiable list, in the order they were listed. */
  List<GcsItemInfo> getChildren() {
    return children;
  }

  /**
   * Returns whether a page returned a child or the directory's own placeholder object, which proves
   * that the directory exists. An empty bucket lists nothing, and so does an empty folder on a
   * hierarchical namespace bucket, so both return {@code false}.
   */
  boolean isDirectoryFound() {
    return directoryFound;
  }
}
