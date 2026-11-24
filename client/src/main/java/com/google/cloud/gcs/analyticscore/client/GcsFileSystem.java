/*
 * Copyright 2025 Google LLC
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

import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

public interface GcsFileSystem extends AutoCloseable {

  /**
   * Opens an object for reading.
   *
   * @param gcsFileInfo Contains information about a GCS File.
   * @param options Fine-grained read options for behaviors of retries, decryption, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  VectoredSeekableByteChannel open(GcsFileInfo gcsFileInfo, GcsReadOptions options)
      throws IOException;

  /**
   * Opens an object for reading.
   *
   * @param gcsItemId gcs object identifier.
   * @param options Fine-grained read options for behaviors of retries, decryption, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  VectoredSeekableByteChannel open(GcsItemId gcsItemId, GcsReadOptions options) throws IOException;

  /**
   * Gets metadata for the given path, which may be an object, a directory, a bucket or the root.
   *
   * <p>As in hadoop-connectors, the bucket is the authority of {@code path} and the object name is
   * its percent-decoded path ({@link URI#getPath()}) without the leading {@code /}: {@code
   * gs://bucket/a%20b.txt} names the object {@code a b.txt}. In an object name, write {@code %} as
   * {@code %25}, {@code ?} as {@code %3F} and {@code #} as {@code %23}; {@link GcsItemId#toUri()}
   * builds such a URI from a name.
   *
   * @param path the {@code gs://} path to look up
   * @return metadata for the item at {@code path}; its {@link GcsFileInfo#getUri() URI} is built
   *     from the name of the item, so it can differ from {@code path} in form (the URI of a bucket
   *     always ends with {@code /})
   * @throws IllegalArgumentException if {@code path} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes), or if it has a query or a fragment
   * @throws FileNotFoundException if nothing exists at {@code path}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfo(URI path) throws IOException;

  /**
   * Gets metadata for the given item, which may be an object, a directory, a bucket or the root.
   *
   * @param itemId the item to look up
   * @return metadata for the item
   * @throws FileNotFoundException if nothing exists at {@code itemId}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfo(GcsItemId itemId) throws IOException;

  /**
   * Gets metadata for the object at the given path, without checking for a directory there.
   *
   * <p>Unlike {@link #getFileInfo(URI)}, this sends a single object metadata request and never
   * infers a directory. Use it when only an object is acceptable, for example before opening a file
   * for reading. {@code path} is parsed as in {@link #getFileInfo(URI)}: its path is
   * percent-decoded.
   *
   * @param path the {@code gs://} path of the object to look up
   * @return metadata for the object at {@code path}
   * @throws IllegalArgumentException if {@code path} is the root, a bucket or a directory path
   *     (ending in {@code /}), or is not a valid {@code gs://} URI as described in {@link
   *     #getFileInfo(URI)}
   * @throws FileNotFoundException if no object exists at {@code path}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfoObject(URI path) throws IOException;

  /**
   * Gets metadata for the given object, without checking for a directory at its path.
   *
   * <p>Unlike {@link #getFileInfo(GcsItemId)}, this sends a single object metadata request and
   * never infers a directory. Use it when only an object is acceptable, for example before opening
   * a file for reading.
   *
   * @param itemId the object to look up
   * @return metadata for the object
   * @throws IllegalArgumentException if {@code itemId} is the root, a bucket or a directory (its
   *     object name ends in {@code /})
   * @throws FileNotFoundException if no object exists at {@code itemId}
   * @throws IOException if the metadata cannot be fetched
   */
  GcsFileInfo getFileInfoObject(GcsItemId itemId) throws IOException;

  /**
   * Lists the object, or the immediate children of the directory or bucket, at the given {@code
   * gs://} path.
   *
   * <p>Semantics follow {@link #listFileInfo(GcsItemId)}. {@code path} is parsed as in {@link
   * #getFileInfo(URI)}: its path is percent-decoded.
   *
   * <p>The default implementation parses {@code path} and calls {@link #listFileInfo(GcsItemId)}.
   *
   * @param path the {@code gs://} path of the object, directory or bucket to list
   * @return an unmodifiable list of the listed items, as described in {@link
   *     #listFileInfo(GcsItemId)}
   * @throws IllegalArgumentException if {@code path} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes), or if it has a query or a fragment
   * @throws FileNotFoundException if neither an object nor a directory exists at {@code path}, or
   *     if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to read or list {@code path}
   * @throws UnsupportedOperationException if {@code path} is the root ({@code gs:/}), or if this
   *     implementation does not support {@link #listFileInfo(GcsItemId)}
   * @throws InterruptedIOException if the calling thread is interrupted when this method is called
   *     (no request is sent), before a page of the listing is requested, or while it waits for the
   *     background listing (in all three cases, its interrupt status stays set), or if {@link
   *     #close()} interrupts a running background listing (the calling thread's interrupt status is
   *     not set)
   * @throws IOException if the object, bucket, folder or listing request fails
   */
  default List<GcsFileInfo> listFileInfo(URI path) throws IOException {
    return listFileInfo(UriUtil.getItemIdFromUri(path));
  }

  /**
   * Lists the given object, or the immediate children of the given directory or bucket.
   *
   * <p>If {@code itemId} names an existing object (its object name does not end in {@code /}), the
   * result holds only that object, even if a directory with the same name also exists. Otherwise
   * {@code itemId} is listed as a directory, and the result holds its immediate children:
   *
   * <ul>
   *   <li>Objects, including the placeholder objects of subdirectories (such as {@code dir/sub/}).
   *   <li>Subdirectories without a placeholder object, as {@link
   *       GcsItemInfo.ItemType#INFERRED_DIRECTORY} items. These are implicit directories and, on
   *       hierarchical namespace (HNS) buckets, folders. Such items have no timestamps; use {@link
   *       #getFileInfo(GcsItemId)} for the metadata of a folder.
   * </ul>
   *
   * <p>The placeholder object of the listed directory itself is not returned, so an existing empty
   * directory or bucket gives an empty list. The children are sorted by {@link
   * GcsFileInfo#getUri()}: shorter URI strings first, then in lexicographic order. Each item's URI
   * holds its object name percent-encoded where needed (for example, a space as {@code %20}), as
   * described in {@link GcsFileInfo#getUri()}.
   *
   * <p>A bucket is listed as an HNS bucket only when {@link GcsFileSystemOptions#isHnsApiEnabled()}
   * is {@code true} and the bucket metadata lookup ({@code storage.buckets.get}) succeeds; {@code
   * 401}/{@code 403} falls back to flat. When an HNS bucket is listed as a flat bucket, a folder
   * with no objects under it is not returned, and listing such a folder throws {@link
   * FileNotFoundException}.
   *
   * <p>Listing the root, which means listing buckets, is not supported. Calling this method during
   * or after {@link #close()} has implementation-defined behavior: on {@link GcsFileSystemImpl},
   * object-form paths fail with {@link java.util.concurrent.RejectedExecutionException} once the
   * status executor shuts down, while directory and bucket paths do not use that executor.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}, so that
   * implementations written against earlier versions of this interface keep compiling; {@link
   * GcsFileSystemImpl} overrides it.
   *
   * @param itemId the object, directory or bucket to list; any content generation is ignored
   * @return an unmodifiable list with the object, or with the children of the directory or bucket
   * @throws IllegalArgumentException if {@code itemId} has an empty bucket name with a non-empty
   *     object name, or if the bucket name can't be the authority of a URI
   * @throws FileNotFoundException if neither an object nor a directory exists at {@code itemId}, or
   *     if the bucket does not exist
   * @throws AccessDeniedException if the caller lacks permission to read or list {@code itemId}
   * @throws UnsupportedOperationException if {@code itemId} is the root, or if this implementation
   *     does not support listing
   * @throws InterruptedIOException if the calling thread is interrupted when this method is called
   *     (no request is sent), before a page of the listing is requested, or while it waits for the
   *     background listing (in all three cases, its interrupt status stays set), or if {@link
   *     #close()} interrupts a running background listing (the calling thread's interrupt status is
   *     not set)
   * @throws IOException if the object, bucket, folder or listing request fails
   */
  default List<GcsFileInfo> listFileInfo(GcsItemId itemId) throws IOException {
    throw new UnsupportedOperationException(
        "listFileInfo is not supported by " + getClass().getName());
  }

  /** Retrieve the options that were used to create this GcsFileSystem. */
  GcsFileSystemOptions getFileSystemOptions();

  /** Retrieve the gcs client used to create this GcsFileSystem. */
  GcsClient getGcsClient();

  /** Retrieve the telemetry instance used by this file system. */
  Telemetry getTelemetry();

  /** Returns the cache manager used by this file system. */
  AnalyticsCacheManager getCacheManager();

  /** Close the file system. */
  @Override
  void close();

  /**
   * Creates a new GCS object and returns a WritableByteChannel for writing to it.
   *
   * @param itemId the identity of the GCS object to be created
   * @param options configuration options for controlling upload strategies and integrity checks
   * @return a channel for writing data to the newly created object
   * @throws IOException if an I/O error occurs during channel initialization or translation
   */
  WritableByteChannel create(GcsItemId itemId, GcsWriteOptions options) throws IOException;

  /**
   * Creates the target directory or bucket (and any missing parent folders on hierarchical
   * namespace buckets) at the specified {@code gs://} URI.
   *
   * <p>Semantics follow {@link #mkdirs(GcsItemId)}. {@code path} is parsed as in {@link
   * #getFileInfo(URI)}: its path is percent-decoded, so {@code gs://bucket/a%20b/} creates the
   * directory {@code a b/}.
   *
   * <p>The default implementation parses {@code path} and calls {@link #mkdirs(GcsItemId)}.
   *
   * @param path the {@code gs://} URI of the directory or bucket to create
   * @throws IllegalArgumentException if {@code path} is not a valid {@code gs://} URI (for example,
   *     {@code gs:///} or consecutive slashes), or if it has a query or a fragment
   * @throws FileNotFoundException if creating a directory inside a bucket that does not exist
   * @throws FileAlreadyExistsException if an object already exists at a path prefix (when {@link
   *     GcsFileSystemOptions#isCreateItemsConflictCheckEnabled()} is {@code true}) or a non-empty
   *     object already exists at the flat directory marker path
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws UnsupportedOperationException if this implementation does not support {@link
   *     #mkdirs(GcsItemId)}
   * @throws IOException if an I/O error occurs
   */
  default void mkdirs(URI path) throws IOException {
    mkdirs(UriUtil.getItemIdFromUri(path));
  }

  /**
   * Creates the target directory or bucket for the given {@link GcsItemId}, succeeding idempotently
   * if it already exists:
   *
   * <ul>
   *   <li><b>Root ({@link GcsItemId#ROOT})</b>: returns immediately as a no-op.
   *   <li><b>Bucket</b>: invokes {@link GcsClient#createBucket(String)} with {@link
   *       GcsCreateBucketOptions#DEFAULT} (requiring {@code storage.buckets.create}; HTTP {@code
   *       409 Conflict} on an existing bucket, including one owned by another project, is treated
   *       as success, whereas object-only IAM roles fail with {@link AccessDeniedException}).
   *   <li><b>Directory</b>: on hierarchical namespace (HNS) buckets (when {@link
   *       GcsFileSystemOptions#isHnsApiEnabled()} is {@code true} and bucket metadata lookup via
   *       {@code storage.buckets.get} succeeds; {@code 401}/{@code 403} falls back to flat),
   *       recursively creates native folders via the Storage Control API. On flat buckets, creates
   *       only the leaf 0-byte directory marker object ({@code "dir/"}); if a non-empty object
   *       already exists there, the marker can't be created and {@link FileAlreadyExistsException}
   *       is thrown. HNS buckets succeed in that case, because writing such an object also creates
   *       the native folder.
   * </ul>
   *
   * <p>When {@link GcsFileSystemOptions#isCreateItemsConflictCheckEnabled()} is {@code true},
   * performs a best-effort prefix check ({@code O(depth)} {@code storage.objects.get} lookups) for
   * conflicting files before creating the directory; this check is not atomic with respect to
   * concurrent writes, and it skips paths the caller can't read (for example, outside a Credential
   * Access Boundary or with a role that lacks {@code storage.objects.get}).
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}, so that
   * implementations written against earlier versions of this interface keep compiling; {@link
   * GcsFileSystemImpl} overrides it.
   *
   * @param itemId the GCS item identifier of the directory, bucket, or root to create
   * @throws IllegalArgumentException if {@code itemId} has an empty bucket name with a non-empty
   *     object name, or if the object name starts with {@code "/"} or contains consecutive {@code
   *     "/"} characters
   * @throws FileNotFoundException if creating a directory inside a bucket that does not exist
   * @throws FileAlreadyExistsException if an object already exists at a path prefix (when conflict
   *     checking is enabled) or a non-empty object already exists at the flat directory marker path
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws UnsupportedOperationException if this implementation does not support creating
   *     directories
   * @throws IOException if an I/O error occurs
   */
  default void mkdirs(GcsItemId itemId) throws IOException {
    throw new UnsupportedOperationException("mkdirs is not supported by " + getClass().getName());
  }

  /**
   * Deletes the object, directory, or bucket at the specified {@code gs://} URI.
   *
   * <p>Semantics follow {@link #delete(GcsItemId, boolean)}. {@code path} is parsed as in {@link
   * #getFileInfo(URI)}: its path is percent-decoded, so {@code gs://bucket/a%20b.txt} deletes the
   * object {@code a b.txt}. In an object name, write {@code %} as {@code %25}, {@code ?} as {@code
   * %3F}, and {@code #} as {@code %23}; {@link GcsItemId#toUri()} builds such a URI from a name.
   *
   * <p>The default implementation parses {@code path} and calls {@link #delete(GcsItemId,
   * boolean)}.
   *
   * @param path the {@code gs://} URI of the object, directory, or bucket to delete
   * @param recursive if {@code true} and {@code path} is a directory or bucket, recursively deletes
   *     all contained objects and subdirectories; if {@code false}, fails with {@link
   *     DirectoryNotEmptyException} when non-empty
   * @throws NullPointerException if {@code path} is {@code null}
   * @throws IllegalArgumentException if {@code path} is the GCS root or not a valid {@code gs://}
   *     URI (for example, {@code gs:///} or consecutive slashes), or if it has a query or a
   *     fragment
   * @throws FileNotFoundException if nothing exists at {@code path}
   * @throws DirectoryNotEmptyException if {@code recursive} is {@code false} and the target
   *     directory or bucket is not empty
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws InterruptedIOException if the calling thread is interrupted before or while deleting,
   *     or while waiting on a long-running operation
   * @throws UnsupportedOperationException if this implementation does not support {@link
   *     #delete(GcsItemId, boolean)}
   * @throws IOException if an I/O error occurs
   */
  default void delete(URI path, boolean recursive) throws IOException {
    delete(UriUtil.getItemIdFromUri(path), recursive);
  }

  /**
   * Deletes the GCS object, directory, or bucket identified by {@code itemId}:
   *
   * <ul>
   *   <li><b>Null or root ({@link GcsItemId#ROOT})</b>: rejected with {@link NullPointerException}
   *       (for {@code null}) or {@link IllegalArgumentException} (for {@link GcsItemId#ROOT} or an
   *       empty bucket name).
   *   <li><b>Content generation</b>: any {@link GcsItemId#getContentGeneration()} on the input
   *       {@code itemId} is ignored; generation-match preconditions use the positive content
   *       generation observed from the live metadata lookup or listing.
   *   <li><b>Missing path</b>: throws {@link FileNotFoundException} if nothing exists at {@code
   *       itemId}.
   *   <li><b>Single object</b>: deletes the object via {@link GcsClient#deleteObject(GcsItemId)},
   *       sending a generation-match precondition when a positive content generation is known.
   *   <li><b>Non-recursive directory or bucket ({@code recursive == false})</b>: throws {@link
   *       DirectoryNotEmptyException} if any child object, nested folder, or (on HNS) non-empty
   *       object at the directory path exists; otherwise removes the empty directory (clearing any
   *       0-byte legacy placeholder first on HNS) or bucket.
   *   <li><b>Recursive directory ({@code recursive == true})</b>: on flat-namespace buckets,
   *       deletes all child objects first, then nested directory placeholders from deepest to
   *       shallowest depth, and finally the target directory placeholder itself. On hierarchical
   *       namespace (HNS) buckets, uses the server-side Storage Control {@code
   *       DeleteFolderRecursive} long-running operation when {@link
   *       GcsFileSystemOptions#isHnsServerSideRecursiveDeleteEnabled()} is {@code true} (default),
   *       falling back to client-side deletion (all objects first, then native folders deepest to
   *       shallowest) only if the server rejects the initial request as unimplemented ({@code
   *       UNIMPLEMENTED}) before accepting a long-running operation. If waiting for an accepted
   *       {@code DeleteFolderRecursive} operation times out, fails polling, or is interrupted,
   *       {@link IOException} or {@link InterruptedIOException} is thrown naming the operation
   *       while the server may continue deleting asynchronously; a concurrent {@code NOT_FOUND}
   *       while another recursive delete of the same folder is in flight returns normally. Callers
   *       that require strictly synchronous client-side deletion can set {@code
   *       analytics-core.hierarchical.namespace.server-side-recursive-delete.enable=false}.
   *   <li><b>Bucket</b>: verifies emptiness (non-recursive) or deletes all objects and folders in
   *       the bucket (recursive), and calls {@link GcsClient#deleteBucket(GcsItemId)} only when
   *       {@link GcsFileSystemOptions#isBucketDeleteEnabled()} is {@code true} (default {@code
   *       false}); otherwise bucket deletion is skipped with an info log.
   *   <li><b>Flat implicit parent repair</b>: on flat-namespace buckets when {@link
   *       GcsFileSystemOptions#isAutoRepairImplicitDirectoriesEnabled()} is {@code true} (default),
   *       performs a best-effort check and creates a 0-byte placeholder marker for the immediate
   *       parent directory (if not root, bucket, or malformed/dot-segment path) so the parent
   *       remains visible; repair errors are logged and never fail an already-completed delete.
   * </ul>
   *
   * <p>A bucket is deleted as an HNS bucket only when {@link
   * GcsFileSystemOptions#isHnsApiEnabled()} is {@code true} and the bucket metadata lookup ({@code
   * storage.buckets.get}) succeeds; {@code 401}/{@code 403} falls back to flat. When an HNS bucket
   * is treated as flat, empty native folders without objects cannot be found or deleted, and
   * directory deletes follow flat placeholder semantics.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}, so that
   * implementations written against earlier versions of this interface keep compiling; {@link
   * GcsFileSystemImpl} overrides it.
   *
   * @param itemId the GCS item identifier of the object, directory, or bucket to delete; any
   *     content generation is ignored
   * @param recursive if {@code true} and {@code itemId} is a directory or bucket, recursively
   *     deletes all contained objects and subdirectories; if {@code false}, fails with {@link
   *     DirectoryNotEmptyException} when non-empty
   * @throws NullPointerException if {@code itemId} is {@code null}
   * @throws IllegalArgumentException if {@code itemId} is {@link GcsItemId#ROOT} or has an empty
   *     bucket name
   * @throws FileNotFoundException if nothing exists at {@code itemId}
   * @throws DirectoryNotEmptyException if {@code recursive} is {@code false} and the target
   *     directory or bucket is not empty
   * @throws AccessDeniedException if the caller lacks required GCS permissions
   * @throws InterruptedIOException if the calling thread is interrupted before or while deleting,
   *     or while waiting on a long-running operation (its interrupt status stays set)
   * @throws UnsupportedOperationException if this implementation does not support deleting items
   * @throws IOException if an I/O error occurs
   */
  default void delete(GcsItemId itemId, boolean recursive) throws IOException {
    throw new UnsupportedOperationException("delete is not supported by " + getClass().getName());
  }
}
