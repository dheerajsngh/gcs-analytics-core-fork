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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The immediate children of a directory, listed across every page of {@link
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
   * Lists every page of the immediate children of {@code id.toDirectoryId()}; a bucket {@code id}
   * lists the bucket root.
   *
   * <p>Listed objects are kept as they are, and prefixes become inferred directories. Each object
   * name is kept once across all pages: an object wins over an inferred directory with the same
   * name.
   *
   * @param gcsClient the client that lists each page
   * @param id the directory or bucket to list
   * @param options the options of each page request; they must report the directory's own
   *     placeholder object in {@link GcsListPage#getPrefixObject()} rather than in {@link
   *     GcsListPage#getItems()} ({@link GcsListOptions#isIncludePrefixObject()} is {@code false})
   * @return the listed children, and whether the listing proves that the directory exists
   * @throws FileNotFoundException if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to list the directory
   * @throws IOException if a page cannot be listed
   */
  static DirectoryListing list(GcsClient gcsClient, GcsItemId id, GcsListOptions options)
      throws IOException {
    GcsItemId dirId = id.toDirectoryId();
    // Keyed by object name, so that an object listed on any page wins over a same-named prefix.
    Map<String, GcsItemInfo> children = new LinkedHashMap<>();
    boolean placeholderFound = false;
    Optional<String> pageToken = Optional.empty();
    do {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedIOException("Interrupted while listing " + dirId);
      }
      GcsListPage<GcsItemInfo> page =
          gcsClient.listObjectsPage(dirId, options.toBuilder().setPageToken(pageToken).build());
      placeholderFound |= page.getPrefixObject().isPresent();
      for (GcsItemInfo item : page.getItems()) {
        children.put(item.getItemId().getObjectName().orElse(""), item);
      }
      for (GcsItemId prefix : page.getPrefixes()) {
        children.putIfAbsent(
            prefix.getObjectName().orElse(""), GcsItemInfo.createInferredDirectory(prefix));
      }
      pageToken = page.getNextPageToken();
    } while (pageToken.isPresent());
    return new DirectoryListing(
        List.copyOf(children.values()), placeholderFound || !children.isEmpty());
  }

  /** Returns the immediate children, as an unmodifiable list in unspecified order. */
  List<GcsItemInfo> getChildren() {
    return children;
  }

  /**
   * Returns whether a page returned a child or the directory's own placeholder object, which proves
   * that the directory exists. An empty bucket and an empty native folder (on hierarchical
   * namespace buckets) list nothing, so they return {@code false}.
   */
  boolean isDirectoryFound() {
    return directoryFound;
  }
}
