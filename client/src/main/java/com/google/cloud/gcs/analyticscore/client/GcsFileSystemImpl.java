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

import static com.google.cloud.gcs.analyticscore.client.UriUtil.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.auth.GcsCredentialsFactory;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsFileSystemImpl implements GcsFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemImpl.class);

  /**
   * Using a 30-second keep-alive enables efficient thread reuse during intermittent spikes in
   * status and list requests, while ensuring rapid resource cleanup during periods of inactivity.
   */
  private static final int CACHED_EXECUTOR_KEEP_ALIVE_SECONDS = 30;

  // TODO: Determine the appropriate values for pool size and queue capacity for cached executor by
  // benchmarking.
  /** Max thread pool size for cached status executor, clamped between 16 and 128. */
  private static final int CACHED_EXECUTOR_MAX_POOL_SIZE =
      Math.max(16, Math.min(Runtime.getRuntime().availableProcessors() * 4, 128));

  /** Timeout in seconds for background thread pools to terminate when closing the file system. */
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 10;

  /**
   * Sorts listed items like hadoop-connectors {@code FILE_INFO_PATH_COMPARATOR}: shorter URI
   * strings first, then in lexicographic order.
   */
  private static final Comparator<GcsFileInfo> LIST_FILE_INFO_PATH_COMPARATOR =
      Comparator.comparing(
          (GcsFileInfo fileInfo) -> fileInfo.getUri().toString(),
          Comparator.comparingInt(String::length).thenComparing(Comparator.naturalOrder()));

  private final GcsClient gcsClient;
  private final GcsFileSystemOptions fileSystemOptions;
  private final Supplier<ExecutorService> readExecutorServiceSupplier;
  private final Supplier<ExecutorService> statusExecutorServiceSupplier;
  private final Telemetry telemetry;
  private final AnalyticsCacheManager cacheManager;
  private final FlatNamespaceStrategyImpl flatStrategy;
  private final HierarchicalNamespaceStrategyImpl hnsStrategy;

  /**
   * Creates a file system whose credentials are resolved from {@code fileSystemOptions}.
   *
   * <p>First sets any proxy username and password for the whole JVM; see {@link
   * GcsCredentialsFactory#installProxyCredentialsIfConfigured}.
   *
   * @param fileSystemOptions The file system options, including the authentication options.
   * @throws UncheckedIOException If credentials cannot be resolved from the authentication options.
   */
  public GcsFileSystemImpl(GcsFileSystemOptions fileSystemOptions) {
    this(() -> createCredentials(fileSystemOptions), fileSystemOptions);
  }

  /**
   * Creates a file system that uses {@code credentials} instead of resolving credentials from the
   * authentication options in {@code fileSystemOptions}.
   *
   * <p>First sets any proxy username and password for the whole JVM; see {@link
   * GcsCredentialsFactory#installProxyCredentialsIfConfigured}.
   *
   * @param credentials The credentials for GCS requests.
   * @param fileSystemOptions The file system options.
   */
  public GcsFileSystemImpl(Credentials credentials, GcsFileSystemOptions fileSystemOptions) {
    this(
        Suppliers.ofInstance(checkNotNull(credentials, "credentials should not be null")),
        fileSystemOptions);
  }

  // Takes the JDK Supplier, not Guava's. javac loads the parameter types of every constructor
  // with the same number of arguments when a caller picks an overload, and some callers, such as
  // Apache Iceberg, do not have Guava on their compile classpath.
  private GcsFileSystemImpl(
      java.util.function.Supplier<Credentials> credentialsSupplier,
      GcsFileSystemOptions fileSystemOptions) {
    checkNotNull(fileSystemOptions, "fileSystemOptions should not be null");
    // Must run first. Telemetry, the project ID lookup, and token requests can open the JVM's
    // first HTTP connection.
    GcsCredentialsFactory.installProxyCredentialsIfConfigured(
        fileSystemOptions.getGcsAuthOptions());
    Credentials credentials = credentialsSupplier.get();
    this.fileSystemOptions = fileSystemOptions;
    this.readExecutorServiceSupplier = initializeReadExecutionServiceSupplier();
    this.statusExecutorServiceSupplier = initializeStatusExecutionServiceSupplier();
    this.telemetry = createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions());
    this.cacheManager = new AnalyticsCacheManager(fileSystemOptions.getGcsCacheOptions());
    this.gcsClient =
        telemetry.measure(
            GcsAnalyticsCoreTelemetryConstants.Operation.GCS_CLIENT_CREATE.name(),
            GcsAnalyticsCoreTelemetryConstants.Metric.GCS_CLIENT_CREATE_DURATION,
            Collections.emptyMap(),
            recorder ->
                new GcsClientImpl(
                    credentials,
                    fileSystemOptions.getGcsClientOptions(),
                    readExecutorServiceSupplier,
                    telemetry,
                    cacheManager.getBucketPropertiesCache(),
                    fileSystemOptions.getGcsAuthOptions()));
    this.flatStrategy = new FlatNamespaceStrategyImpl(this.gcsClient, this.fileSystemOptions);
    this.hnsStrategy =
        new HierarchicalNamespaceStrategyImpl(this.gcsClient, this.fileSystemOptions);
  }

  private static Credentials createCredentials(GcsFileSystemOptions fileSystemOptions) {
    try {
      return GcsCredentialsFactory.createCredentials(fileSystemOptions.getGcsAuthOptions());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create credentials from auth options", e);
    }
  }

  @VisibleForTesting
  GcsFileSystemImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this(
        gcsClient,
        fileSystemOptions,
        createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions()),
        new AnalyticsCacheManager(fileSystemOptions.getGcsCacheOptions()));
  }

  @VisibleForTesting
  GcsFileSystemImpl(
      GcsClient gcsClient,
      GcsFileSystemOptions fileSystemOptions,
      Telemetry telemetry,
      AnalyticsCacheManager cacheManager) {
    this.gcsClient = gcsClient;
    this.fileSystemOptions = fileSystemOptions;
    this.readExecutorServiceSupplier = initializeReadExecutionServiceSupplier();
    this.statusExecutorServiceSupplier = initializeStatusExecutionServiceSupplier();
    this.telemetry = telemetry;
    this.cacheManager = cacheManager;
    this.flatStrategy = new FlatNamespaceStrategyImpl(this.gcsClient, this.fileSystemOptions);
    this.hnsStrategy =
        new HierarchicalNamespaceStrategyImpl(this.gcsClient, this.fileSystemOptions);
  }

  @VisibleForTesting
  NamespaceStrategy resolveStrategy(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    if (!fileSystemOptions.isHnsApiEnabled()) {
      return flatStrategy;
    }

    BucketProperties properties = gcsClient.getBucketProperties(bucketName);

    if (properties.isHnsEnabled()) {
      return hnsStrategy;
    }
    return flatStrategy;
  }

  @Override
  public VectoredSeekableByteChannel open(GcsFileInfo gcsFileInfo, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsFileInfo, "fileInfo should not be null");
    GcsItemInfo itemInfo = checkNotNull(gcsFileInfo.getItemInfo(), "itemInfo should not be null");
    GcsItemId itemId = checkNotNull(itemInfo.getItemId(), "itemId should not be null");
    checkArgument(itemId.isGcsObject(), "Expected GCS object to be provided. But got: %s", itemId);
    return gcsClient.openReadChannel(itemInfo, readOptions);
  }

  @Override
  public VectoredSeekableByteChannel open(GcsItemId gcsItemId, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkArgument(
        gcsItemId.isGcsObject(), "Expected GCS object to be provided. But got: %s", gcsItemId);
    return gcsClient.openReadChannel(gcsItemId, readOptions);
  }

  @Override
  public GcsFileInfo getFileInfo(URI path) throws IOException {
    checkNotNull(path, "path should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromUri(path);
    return getFileInfo(itemId);
  }

  @Override
  public GcsFileInfo getFileInfo(GcsItemId itemId) throws IOException {
    return getFileInfoInternal(itemId, /* inferImplicitDirectories= */ true);
  }

  @Override
  public GcsFileInfo getFileInfoObject(URI path) throws IOException {
    checkNotNull(path, "path should not be null");
    return getFileInfoObject(UriUtil.getItemIdFromUri(path));
  }

  @Override
  public GcsFileInfo getFileInfoObject(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId should not be null");
    checkArgument(
        itemId.isGcsObject() && itemId.resolvePathType() != PathType.DIRECTORY,
        "Expected GCS object to be provided. But got: %s",
        itemId);
    return toGcsFileInfo(gcsClient.getGcsItemInfo(itemId));
  }

  @Override
  public List<GcsFileInfo> listFileInfo(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId should not be null");
    if (itemId.isRoot()) {
      // Listing the root means listing buckets, which GcsClient does not support yet.
      throw new UnsupportedOperationException("Listing the root (gs:/) is not supported");
    }
    checkArgument(
        !itemId.getBucketName().isEmpty(),
        "Expected a bucket, directory or object but got: %s",
        itemId);
    // Listing ignores content generation; strip it once so lookups and error messages omit it.
    GcsItemId targetId = UriUtil.withoutContentGeneration(itemId);
    // Checked here, before any request: a listing handed to the status executor would not see the
    // interrupt of this thread, and a finished lookup returns without checking for it.
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException(
          "Interrupted before listing " + UriUtil.getStringPath(targetId));
    }

    if (targetId.isDirectory()) {
      return toSortedFileInfos(listDirectory(targetId));
    }

    // An object wins over a directory with the same name. Fetch only the first listing page in the
    // background (or lazily, if parallel metadata lookup is disabled) while looking up the object,
    // so multi-page listings do not hold a status-pool thread past the first page.
    ExecutorService statusExecutorService = statusExecutorServiceSupplier.get();
    Future<StartedDirectoryListing> firstPageFuture =
        statusExecutorService.submit(() -> startDirectoryListing(targetId));
    boolean awaitListing = false;
    try {
      GcsItemInfo objectInfo = gcsClient.getGcsItemInfo(targetId);
      return List.of(toGcsFileInfo(objectInfo));
    } catch (FileNotFoundException ignored) {
      LOG.debug("Object '{}' not found, listing it as a directory.", targetId);
      awaitListing = true;
    } finally {
      if (!awaitListing) {
        firstPageFuture.cancel(true);
      }
    }
    StartedDirectoryListing started = getFromFuture(firstPageFuture);
    return toSortedFileInfos(started.complete(targetId));
  }

  private List<GcsItemInfo> listDirectory(GcsItemId itemId) throws IOException {
    NamespaceStrategy strategy = resolveStrategy(itemId.getBucketName());
    return strategy.listDirectory(itemId);
  }

  private StartedDirectoryListing startDirectoryListing(GcsItemId itemId) throws IOException {
    NamespaceStrategy strategy = resolveStrategy(itemId.getBucketName());
    return new StartedDirectoryListing(strategy, strategy.listDirectoryFirstPage(itemId));
  }

  private static final class StartedDirectoryListing {
    private final NamespaceStrategy strategy;
    private final GcsListPage<GcsItemInfo> firstPage;

    private StartedDirectoryListing(
        NamespaceStrategy strategy, GcsListPage<GcsItemInfo> firstPage) {
      this.strategy = strategy;
      this.firstPage = firstPage;
    }

    private List<GcsItemInfo> complete(GcsItemId itemId) throws IOException {
      return strategy.completeDirectoryListing(itemId, firstPage);
    }
  }

  private List<GcsFileInfo> toSortedFileInfos(List<GcsItemInfo> itemInfos) {
    return itemInfos.stream()
        .map(this::toGcsFileInfo)
        .sorted(LIST_FILE_INFO_PATH_COMPARATOR)
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Retrieves file or directory metadata for the given {@link GcsItemId}.
   *
   * @param itemId the item identifier
   * @param inferImplicitDirectories if {@code false}, does not list prefix objects to infer
   *     implicit directories and instead only checks for an explicit placeholder directory object
   *     (needed for operations such as rename and delete)
   */
  @VisibleForTesting
  GcsFileInfo getFileInfoInternal(GcsItemId itemId, boolean inferImplicitDirectories)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");
    PathType pathType = itemId.resolvePathType();

    if (pathType == PathType.ROOT) {
      return GcsFileInfo.ROOT_INFO;
    }

    if (pathType == PathType.BUCKET) {
      GcsItemInfo bucketInfo = gcsClient.getBucketInfo(itemId);
      return createBucketFileInfo(bucketInfo);
    }

    if (pathType == PathType.DIRECTORY) {
      return toGcsFileInfo(lookupDirectoryInfo(itemId, inferImplicitDirectories));
    }

    // Submit directory info in background
    ExecutorService statusExecutorService = statusExecutorServiceSupplier.get();
    Future<GcsItemInfo> directoryInfoFuture =
        statusExecutorService.submit(() -> lookupDirectoryInfo(itemId, inferImplicitDirectories));

    // Perform direct object metadata lookup
    try {
      GcsItemInfo itemInfo = gcsClient.getGcsItemInfo(itemId);
      directoryInfoFuture.cancel(true);
      return toGcsFileInfo(itemInfo);
    } catch (FileNotFoundException ignored) {
      // Direct object not found; fall through to directory info
      LOG.debug("Item '{}' not found directly, checking for directory existence.", itemId);
    } catch (Exception e) {
      directoryInfoFuture.cancel(true);
      throw e;
    }

    // Await directory info and return (unwraps ExecutionException to FileNotFoundException /
    // IOException)
    return toGcsFileInfo(getFromFuture(directoryInfoFuture));
  }

  private GcsItemInfo lookupDirectoryInfo(GcsItemId itemId, boolean inferImplicitDirectories)
      throws IOException {
    NamespaceStrategy strategy = resolveStrategy(itemId.getBucketName());
    return strategy.getDirectoryInfo(itemId, inferImplicitDirectories);
  }

  static <T> T getFromFuture(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw (InterruptedIOException)
          new InterruptedIOException("Thread interrupted while waiting on future").initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      Throwables.throwIfInstanceOf(cause, IOException.class);
      Throwables.throwIfUnchecked(cause);
      throw new IOException(cause);
    }
  }

  private GcsFileInfo createBucketFileInfo(GcsItemInfo bucketInfo) {
    return GcsFileInfo.builder()
        .setItemInfo(bucketInfo)
        .setUri(UriUtil.toUri(bucketInfo.getItemId()))
        .setAttributes(Collections.emptyMap())
        .build();
  }

  private GcsFileInfo toGcsFileInfo(GcsItemInfo gcsItemInfo) {
    return GcsFileInfo.builder()
        .setItemInfo(gcsItemInfo)
        .setUri(UriUtil.toUri(gcsItemInfo.getItemId()))
        .setAttributes(gcsItemInfo.getExtendedAttributes())
        .build();
  }

  @Override
  public GcsFileSystemOptions getFileSystemOptions() {
    return this.fileSystemOptions;
  }

  @Override
  public GcsClient getGcsClient() {
    return this.gcsClient;
  }

  @Override
  public Telemetry getTelemetry() {
    return telemetry;
  }

  @Override
  public AnalyticsCacheManager getCacheManager() {
    return cacheManager;
  }

  @VisibleForTesting
  FlatNamespaceStrategyImpl getFlatStrategy() {
    return flatStrategy;
  }

  @VisibleForTesting
  HierarchicalNamespaceStrategyImpl getHnsStrategy() {
    return hnsStrategy;
  }

  @Override
  public void close() {
    ExecutorService readExecutorService = readExecutorServiceSupplier.get();
    ExecutorService statusExecutorService = statusExecutorServiceSupplier.get();
    readExecutorService.shutdown();
    statusExecutorService.shutdown();
    try {
      // Wait a total of EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS for both thread pools to terminate.
      long deadline =
          System.nanoTime() + TimeUnit.SECONDS.toNanos(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
      // First, wait for the read executor service to terminate.
      if (!readExecutorService.awaitTermination(
          EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        readExecutorService.shutdownNow();
      }
      // Then, wait for the status executor service to terminate, with the remaining time.
      if (!statusExecutorService.awaitTermination(
          Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
        statusExecutorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      readExecutorService.shutdownNow();
      statusExecutorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    gcsClient.close();
    telemetry.close();
  }

  @Override
  public WritableByteChannel create(GcsItemId itemId, GcsWriteOptions writeOptions)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");

    // Delegate the actual SDK interaction and exception handling to the internal client
    return gcsClient.createWriteChannel(itemId, writeOptions);
  }

  @Override
  public void mkdirs(GcsItemId itemId) throws IOException {
    checkNotNull(itemId, "itemId should not be null");
    if (itemId.isRoot()) {
      // Root path always exists.
      return;
    }
    if (itemId.isBucket()) {
      try {
        gcsClient.createBucket(itemId.getBucketName());
      } catch (FileAlreadyExistsException e) {
        LOG.debug("Bucket {} already exists.", itemId.getBucketName(), e);
      }
      return;
    }

    UriUtil.validateDirectoryItemId(itemId);
    GcsItemId dirItemId = itemId.toDirectoryId();

    NamespaceStrategy strategy = resolveStrategy(itemId.getBucketName());

    if (fileSystemOptions.isCreateItemsConflictCheckEnabled()) {
      checkNoFilesConflictingWithDirs(dirItemId);
    }

    strategy.createDirectory(dirItemId);
  }

  @Override
  public void rename(GcsItemId src, GcsItemId dst) throws IOException {
    checkNotNull(src, "src should not be null");
    checkNotNull(dst, "dst should not be null");
    checkArgument(!src.isRoot(), "Root path cannot be renamed.");
    checkArgument(
        src.isBucket() || src.isGcsObject(),
        "Expected a bucket or GCS object for src, but got: %s",
        src);
    checkArgument(
        dst.isRoot() || dst.isBucket() || dst.isGcsObject(),
        "Expected root, bucket, or GCS object for dst, but got: %s",
        dst);
    if (dst.isGcsObject()) {
      String dstObjectName = dst.getObjectName().get();
      checkArgument(
          !dstObjectName.startsWith(PATH_DELIMITER)
              && !dstObjectName.contains(PATH_DELIMITER + PATH_DELIMITER),
          "Destination object name cannot start with '/' or contain consecutive '/': %s",
          dst);
    }

    if (src.isBucket()) {
      throw new UnsupportedOperationException(
          "Renaming a bucket is not supported: " + UriUtil.getStringPath(src));
    }
    if (!dst.isRoot() && UriUtil.hasDotSegment(dst.getObjectName().orElse(""))) {
      throw new IOException(
          String.format(
              "Refusing to rename '%s' to '%s': destination contains '.' or '..' path segment",
              UriUtil.getStringPath(src), UriUtil.getStringPath(dst)));
    }
    if (!dst.isRoot() && !src.getBucketName().equals(dst.getBucketName())) {
      throw new UnsupportedOperationException(
          String.format(
              "Cross-bucket rename is not supported: '%s' to '%s'",
              UriUtil.getStringPath(src), UriUtil.getStringPath(dst)));
    }

    GcsItemId srcLookupId = UriUtil.withoutContentGeneration(src);
    GcsItemId dstLookupId = UriUtil.withoutContentGeneration(dst);
    NamespaceStrategy strategy = resolveStrategy(srcLookupId.getBucketName());
    RenameMetadata metadata = lookupRenameTargets(strategy, srcLookupId, dstLookupId);

    GcsItemId srcId = UriUtil.withoutContentGeneration(metadata.srcInfo.getItemInfo().getItemId());
    boolean srcIsDir = isDirectory(metadata.srcInfo.getItemInfo());
    GcsItemId resolvedDst = resolveRenameDestination(srcId, srcIsDir, metadata);

    if (srcId.equals(resolvedDst)) {
      return;
    }
    if (metadata.dstExists) {
      checkResolvedDestinationDoesNotExist(strategy, resolvedDst);
    }

    GcsItemId srcParentToRepair = null;
    Future<Optional<GcsItemInfo>> srcParentProbeFuture = null;
    if (strategy.shouldRepairImplicitParentDirectory()
        && fileSystemOptions.isAutoRepairImplicitDirectoriesEnabled()) {
      Optional<GcsItemId> srcParentOpt = UriUtil.getParentId(srcId);
      if (srcParentOpt.isPresent() && srcParentOpt.get().isGcsObject()) {
        GcsItemId parentId = srcParentOpt.get();
        String parentName = parentId.getObjectName().orElse("");
        if (!parentName.startsWith(PATH_DELIMITER)
            && !parentName.contains(PATH_DELIMITER + PATH_DELIMITER)
            && !UriUtil.hasDotSegment(parentName)) {
          srcParentToRepair = parentId;
          try {
            srcParentProbeFuture =
                statusExecutorServiceSupplier
                    .get()
                    .submit(
                        () ->
                            lookupDirectoryInfoOptional(
                                strategy, parentId, /* inferImplicitDirectories= */ false));
          } catch (RejectedExecutionException e) {
            LOG.warn(
                "Skipping implicit parent directory repair in bucket {} because executor is shut"
                    + " down",
                parentId.getBucketName());
            LOG.debug(
                "Skipped implicit parent directory repair for {}",
                UriUtil.getStringPath(parentId),
                e);
          }
        }
      }
    }

    try {
      if (srcIsDir) {
        strategy.renameDirectory(srcId, resolvedDst);
      } else {
        moveObjectWithRetryRecovery(gcsClient, metadata.srcInfo.getItemInfo(), resolvedDst);
      }
    } catch (Throwable t) {
      cancelIfPresent(srcParentProbeFuture);
      throw t;
    }

    if (srcParentProbeFuture != null) {
      repairImplicitParentDirectory(srcParentToRepair, srcParentProbeFuture);
    }
  }

  private RenameMetadata lookupRenameTargets(
      NamespaceStrategy strategy, GcsItemId srcLookupId, GcsItemId dstLookupId) throws IOException {
    Optional<GcsItemId> dstParentOpt = UriUtil.getParentId(dstLookupId);
    ExecutorService statusExecutor = statusExecutorServiceSupplier.get();

    Future<Optional<GcsItemInfo>> dstParentFuture = null;
    Future<Optional<GcsItemInfo>> dstObjFuture = null;
    Future<Optional<GcsItemInfo>> dstDirFuture = null;
    GcsItemId nonDirDstId = null;
    try {
      // Submit only leaf, non-reentrant RPC tasks onto statusExecutor so worker threads can never
      // block awaiting other tasks in the same pool. Skip getBucketInfo when dst or dstParent is
      // src's bucket to preserve object-only IAM / CAB compatibility.
      if (dstParentOpt.isPresent() && dstParentOpt.get().isGcsObject()) {
        GcsItemId dstParentDirId = dstParentOpt.get().toDirectoryId();
        dstParentFuture =
            statusExecutor.submit(
                () ->
                    lookupDirectoryInfoOptional(
                        strategy, dstParentDirId, /* inferImplicitDirectories= */ true));
      }
      if (dstLookupId.isGcsObject()) {
        if (dstLookupId.resolvePathType() == PathType.DIRECTORY) {
          dstDirFuture =
              statusExecutor.submit(
                  () ->
                      lookupDirectoryInfoOptional(
                          strategy, dstLookupId, /* inferImplicitDirectories= */ true));
          String nonDirName =
              UriUtil.removeTrailingSlash(dstLookupId.getObjectName().orElseThrow());
          if (!nonDirName.isEmpty() && !nonDirName.endsWith(PATH_DELIMITER)) {
            GcsItemId strippedDstId =
                GcsItemId.builder()
                    .setBucketName(dstLookupId.getBucketName())
                    .setObjectName(nonDirName)
                    .build();
            nonDirDstId = strippedDstId;
            dstObjFuture = statusExecutor.submit(() -> lookupObjectInfoOptional(strippedDstId));
          }
        } else {
          dstObjFuture = statusExecutor.submit(() -> lookupObjectInfoOptional(dstLookupId));
          dstDirFuture =
              statusExecutor.submit(
                  () ->
                      lookupDirectoryInfoOptional(
                          strategy, dstLookupId, /* inferImplicitDirectories= */ true));
        }
      }

      GcsFileInfo srcInfo = getFileInfo(srcLookupId);
      boolean srcIsDir = isDirectory(srcInfo.getItemInfo());

      Optional<GcsItemInfo> dstInfo;
      boolean dstExists;
      Optional<GcsItemId> dstConflictingFileId = Optional.empty();
      if (dstLookupId.isRoot() || dstLookupId.isBucket()) {
        dstInfo = Optional.empty();
        dstExists = true;
      } else if (dstLookupId.resolvePathType() == PathType.DIRECTORY) {
        dstInfo = getFromFuture(dstDirFuture);
        dstExists = dstInfo.isPresent();
        if (dstExists || !srcIsDir) {
          cancelIfPresent(dstObjFuture);
        } else if (dstObjFuture != null) {
          try {
            dstConflictingFileId =
                getFromFuture(dstObjFuture)
                    .map(info -> UriUtil.withoutContentGeneration(info.getItemId()));
          } catch (AccessDeniedException e) {
            // The non-directory probe is best-effort under prefix-scoped Credential Access
            // Boundaries, where objects/<dir>/ is allowed but objects/<dir> is outside the rule.
            LOG.warn(
                "Skipping conflicting-file check for trailing-slash rename destination in bucket {}"
                    + " because the caller cannot read the non-directory path",
                dstLookupId.getBucketName());
            LOG.debug(
                "Conflicting-file check access denied for '{}'",
                UriUtil.getStringPath(nonDirDstId),
                e);
            dstConflictingFileId = Optional.empty();
          }
        }
      } else if (dstObjFuture != null) {
        Optional<GcsItemInfo> objInfo = getFromFuture(dstObjFuture);
        if (objInfo.isPresent()) {
          cancelIfPresent(dstDirFuture);
          dstInfo = objInfo;
        } else {
          dstInfo = getFromFuture(dstDirFuture);
        }
        dstExists = dstInfo.isPresent();
      } else {
        dstInfo = getFromFuture(dstDirFuture);
        dstExists = dstInfo.isPresent();
      }

      GcsItemId resolvedDstLookupId =
          dstInfo
              .map(info -> UriUtil.withoutContentGeneration(info.getItemId()))
              .orElse(dstLookupId);
      boolean dstIsDir =
          dstInfo.map(GcsFileSystemImpl::isDirectory).orElse(dstLookupId.isDirectory());

      // If dst (or a conflicting file at dst without trailing slash) already exists, its parent
      // necessarily exists in GCS; only await dstParentFuture otherwise.
      boolean dstParentExists =
          dstExists
              || dstConflictingFileId.isPresent()
              || dstParentFuture == null
              || getFromFuture(dstParentFuture).isPresent();

      return new RenameMetadata(
          srcInfo,
          resolvedDstLookupId,
          dstExists,
          dstIsDir,
          dstConflictingFileId,
          dstParentOpt,
          dstParentExists);
    } finally {
      cancelIfPresent(dstParentFuture);
      cancelIfPresent(dstObjFuture);
      cancelIfPresent(dstDirFuture);
    }
  }

  private GcsItemId resolveRenameDestination(
      GcsItemId srcId, boolean srcIsDir, RenameMetadata metadata) throws IOException {
    GcsItemId dstId = metadata.dstId;
    if (!srcIsDir && dstId.isRoot()) {
      throw new IOException("A file cannot be created in root.");
    }
    if (metadata.dstExists && !metadata.dstIsDir && (srcIsDir || !dstId.equals(srcId))) {
      throw new FileAlreadyExistsException(
          UriUtil.getStringPath(dstId), null, "Cannot overwrite an existing file");
    }
    if (srcIsDir && !metadata.dstExists && metadata.dstConflictingFileId.isPresent()) {
      throw GcsExceptionUtil.createFileAlreadyExistsException(
          dstId.toDirectoryId(),
          metadata.dstConflictingFileId.orElseThrow(),
          "Cannot overwrite an existing file");
    }
    if (!metadata.dstParentExists) {
      throw new FileNotFoundException(
          "Cannot rename because destination parent does not exist: "
              + UriUtil.getStringPath(metadata.dstParentOpt.orElseThrow()));
    }

    String srcLeafName = UriUtil.getLeafName(srcId).orElse("");
    if (srcIsDir) {
      GcsItemId dstDirId = dstId.toDirectoryId();
      if (srcId.equals(dstDirId)) {
        throw new IOException("Rename dir to self is forbidden: " + UriUtil.getStringPath(srcId));
      }
      if (UriUtil.isSubdirectory(srcId, dstDirId)) {
        throw new IOException(
            String.format(
                "Rename to subdir is forbidden: '%s' to '%s'",
                UriUtil.getStringPath(srcId), UriUtil.getStringPath(dstDirId)));
      }
      if (metadata.dstExists) {
        if (dstDirId.isRoot()) {
          throw new UnsupportedOperationException(
              "Cannot rename directory into root: " + UriUtil.getStringPath(srcId));
        }
        return UriUtil.resolveChildId(dstDirId, srcLeafName, srcId, /* isDirectory= */ true);
      }
      return dstDirId;
    }

    if (dstId.isDirectory()) {
      if (!metadata.dstExists) {
        throw new FileNotFoundException(
            "Cannot rename because path does not exist: " + UriUtil.getStringPath(dstId));
      }
      return UriUtil.resolveChildId(dstId, srcLeafName, srcId, /* isDirectory= */ false);
    }
    return dstId;
  }

  private void checkResolvedDestinationDoesNotExist(
      NamespaceStrategy strategy, GcsItemId resolvedDst) throws IOException {
    if (resolvedDst.isDirectory()) {
      String nonDirName = UriUtil.removeTrailingSlash(resolvedDst.getObjectName().get());
      GcsItemId nonDirId =
          GcsItemId.builder()
              .setBucketName(resolvedDst.getBucketName())
              .setObjectName(nonDirName)
              .build();
      Future<Optional<GcsItemInfo>> nonDirFuture =
          statusExecutorServiceSupplier.get().submit(() -> lookupObjectInfoOptional(nonDirId));
      boolean awaitNonDir = false;
      try {
        Optional<GcsItemInfo> dirInfo =
            lookupDirectoryInfoOptional(
                strategy, resolvedDst, /* inferImplicitDirectories= */ true);
        if (dirInfo.isPresent()) {
          GcsItemId existingId = UriUtil.withoutContentGeneration(dirInfo.get().getItemId());
          if (existingId.equals(resolvedDst)) {
            throw new FileAlreadyExistsException(
                UriUtil.getStringPath(resolvedDst),
                null,
                "Cannot rename because destination already exists");
          }
          throw GcsExceptionUtil.createFileAlreadyExistsException(
              resolvedDst, existingId, "Cannot rename because destination already exists");
        }
        awaitNonDir = true;
      } finally {
        if (!awaitNonDir) {
          nonDirFuture.cancel(true);
        }
      }
      try {
        Optional<GcsItemInfo> nonDirInfo = getFromFuture(nonDirFuture);
        if (nonDirInfo.isPresent()) {
          GcsItemId existingId = UriUtil.withoutContentGeneration(nonDirInfo.get().getItemId());
          throw GcsExceptionUtil.createFileAlreadyExistsException(
              resolvedDst, existingId, "Cannot rename because destination already exists");
        }
      } catch (AccessDeniedException e) {
        // The stripped non-directory probe is best-effort under prefix-scoped Credential Access
        // Boundaries, where objects/<dir>/ is allowed but objects/<dir> is outside the rule.
        LOG.warn(
            "Skipping conflicting-file check for resolved destination in bucket {} because the"
                + " caller cannot read the non-directory path",
            resolvedDst.getBucketName());
        LOG.debug(
            "Conflicting-file check access denied for resolved destination '{}'",
            UriUtil.getStringPath(nonDirId),
            e);
      }
      return;
    }

    try {
      GcsFileInfo existingInfo = getFileInfo(resolvedDst);
      GcsItemId existingId =
          UriUtil.withoutContentGeneration(existingInfo.getItemInfo().getItemId());
      if (existingId.equals(resolvedDst)) {
        throw new FileAlreadyExistsException(
            UriUtil.getStringPath(resolvedDst),
            null,
            "Cannot rename because destination already exists");
      }
      throw GcsExceptionUtil.createFileAlreadyExistsException(
          resolvedDst, existingId, "Cannot rename because destination already exists");
    } catch (FileNotFoundException ignored) {
      // Resolved destination does not exist; safe to proceed.
    }
  }

  private void repairImplicitParentDirectory(
      GcsItemId parentId, Future<Optional<GcsItemInfo>> parentProbeFuture) {
    Optional<GcsItemInfo> parentInfo;
    try {
      parentInfo = getFromFuture(parentProbeFuture);
    } catch (InterruptedIOException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "Failed to check implicit parent directory in bucket {} after rename; continuing",
          parentId.getBucketName());
      LOG.debug("Implicit parent directory check failed for '{}'", parentId, e);
      return;
    } catch (IOException | RuntimeException e) {
      LOG.warn(
          "Failed to check implicit parent directory in bucket {} after rename; continuing",
          parentId.getBucketName());
      LOG.debug("Implicit parent directory check failed for '{}'", parentId, e);
      return;
    }

    if (parentInfo.isPresent()) {
      return;
    }

    try {
      gcsClient.createEmptyObject(parentId);
    } catch (InterruptedIOException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "Failed to repair implicit parent directory in bucket {} after rename; continuing",
          parentId.getBucketName());
      LOG.debug("Implicit parent directory repair failed for '{}'", parentId, e);
    } catch (IOException | RuntimeException e) {
      LOG.warn(
          "Failed to repair implicit parent directory in bucket {} after rename; continuing",
          parentId.getBucketName());
      LOG.debug("Implicit parent directory repair failed for '{}'", parentId, e);
    }
  }

  /**
   * Moves {@code srcItemInfo} to {@code dstId} with {@code dstGen=0L} (enabling GCS SDK retries and
   * overwrite protection) and recovers from lost-response retries when destination checksum
   * verification proves the earlier attempt already moved the object.
   */
  static void moveObjectWithRetryRecovery(
      GcsClient gcsClient, GcsItemInfo srcItemInfo, GcsItemId dstId) throws IOException {
    checkNotNull(gcsClient, "gcsClient should not be null");
    checkNotNull(srcItemInfo, "srcItemInfo should not be null");
    checkNotNull(dstId, "dstId should not be null");

    GcsItemId srcIdWithoutGen = UriUtil.withoutContentGeneration(srcItemInfo.getItemId());
    Optional<Long> srcGeneration =
        srcItemInfo
            .getContentGeneration()
            .or(() -> srcItemInfo.getItemId().getContentGeneration())
            .filter(gen -> gen > 0L);
    GcsItemId.Builder srcIdBuilder =
        GcsItemId.builder()
            .setBucketName(srcIdWithoutGen.getBucketName())
            .setObjectName(srcIdWithoutGen.getObjectName().get());
    srcGeneration.ifPresent(srcIdBuilder::setContentGeneration);
    GcsItemId srcIdWithGen = srcIdBuilder.build();

    GcsItemId dstIdWithoutGen = UriUtil.withoutContentGeneration(dstId);
    GcsItemId dstIdWithGen0 =
        GcsItemId.builder()
            .setBucketName(dstIdWithoutGen.getBucketName())
            .setObjectName(dstIdWithoutGen.getObjectName().get())
            .setContentGeneration(0L)
            .build();

    try {
      gcsClient.moveObject(srcIdWithGen, dstIdWithGen0);
    } catch (FileNotFoundException e) {
      if (!hasChecksum(srcItemInfo)) {
        throw e;
      }
      GcsItemInfo dstInfo;
      try {
        dstInfo = gcsClient.getGcsItemInfo(dstIdWithoutGen);
      } catch (IOException probeFailure) {
        e.addSuppressed(probeFailure);
        throw e;
      }
      if (checksumsMatch(srcItemInfo, dstInfo)) {
        LOG.debug(
            "Recovered from 404 on retried move {} -> {}: destination matches source checksum.",
            srcIdWithoutGen,
            dstIdWithoutGen);
        return;
      }
      throw e;
    } catch (IOException e) {
      if (!isPreconditionFailed(e) || !hasChecksum(srcItemInfo)) {
        throw e;
      }
      GcsItemInfo dstInfo;
      try {
        dstInfo = gcsClient.getGcsItemInfo(dstIdWithoutGen);
      } catch (IOException probeFailure) {
        e.addSuppressed(probeFailure);
        throw e;
      }
      if (!checksumsMatch(srcItemInfo, dstInfo)) {
        throw e;
      }
      try {
        GcsItemInfo liveSrcInfo = gcsClient.getGcsItemInfo(srcIdWithoutGen);
        Optional<Long> liveSrcGen =
            liveSrcInfo
                .getContentGeneration()
                .or(() -> liveSrcInfo.getItemId().getContentGeneration())
                .filter(gen -> gen > 0L);
        if (srcGeneration.isPresent()
            && liveSrcGen.isPresent()
            && !srcGeneration.get().equals(liveSrcGen.get())) {
          LOG.debug(
              "Recovered from 412 on retried move {} -> {}: destination matches source checksum"
                  + " and source generation changed ({} -> {}).",
              srcIdWithoutGen,
              dstIdWithoutGen,
              srcGeneration.get(),
              liveSrcGen.get());
          return;
        }
      } catch (FileNotFoundException ignored) {
        LOG.debug(
            "Recovered from 412 on retried move {} -> {}: destination matches source checksum and"
                + " source no longer exists.",
            srcIdWithoutGen,
            dstIdWithoutGen);
        return;
      } catch (IOException probeFailure) {
        e.addSuppressed(probeFailure);
        throw e;
      }
      throw e;
    }
  }

  private Optional<GcsItemInfo> lookupObjectInfoOptional(GcsItemId itemId) throws IOException {
    try {
      return Optional.of(gcsClient.getGcsItemInfo(itemId));
    } catch (FileNotFoundException ignored) {
      return Optional.empty();
    }
  }

  private Optional<GcsItemInfo> lookupDirectoryInfoOptional(
      NamespaceStrategy strategy, GcsItemId itemId, boolean inferImplicitDirectories)
      throws IOException {
    try {
      return Optional.of(strategy.getDirectoryInfo(itemId, inferImplicitDirectories));
    } catch (FileNotFoundException ignored) {
      return Optional.empty();
    }
  }

  private static boolean isDirectory(GcsItemInfo itemInfo) {
    switch (itemInfo.getItemType()) {
      case INFERRED_DIRECTORY:
      case PLACEHOLDER_DIRECTORY:
      case NATIVE_FOLDER:
      case BUCKET:
      case ROOT:
        return true;
      case OBJECT:
        return false;
    }
    throw new AssertionError("Unexpected item type: " + itemInfo.getItemType());
  }

  private static boolean isPreconditionFailed(IOException e) {
    return GcsExceptionUtil.getStorageException(e)
        .map(
            se ->
                GcsExceptionUtil.getErrorType(se) == GcsExceptionUtil.ErrorType.PRECONDITION_FAILED)
        .orElse(false);
  }

  private static boolean hasChecksum(GcsItemInfo itemInfo) {
    return itemInfo
        .getVerificationAttributes()
        .map(attrs -> isNonEmpty(attrs.getCrc32c()) || isNonEmpty(attrs.getMd5hash()))
        .orElse(false);
  }

  private static boolean checksumsMatch(GcsItemInfo srcInfo, GcsItemInfo dstInfo) {
    if (srcInfo.getVerificationAttributes().isEmpty()
        || dstInfo.getVerificationAttributes().isEmpty()) {
      return false;
    }
    VerificationAttributes src = srcInfo.getVerificationAttributes().get();
    VerificationAttributes dst = dstInfo.getVerificationAttributes().get();
    boolean srcHasCrc = isNonEmpty(src.getCrc32c());
    boolean dstHasCrc = isNonEmpty(dst.getCrc32c());
    boolean srcHasMd5 = isNonEmpty(src.getMd5hash());
    boolean dstHasMd5 = isNonEmpty(dst.getMd5hash());
    if (srcHasCrc != dstHasCrc || srcHasMd5 != dstHasMd5) {
      return false;
    }
    if (!srcHasCrc && !srcHasMd5) {
      return false;
    }
    if (srcHasCrc && !Arrays.equals(src.getCrc32c(), dst.getCrc32c())) {
      return false;
    }
    if (srcHasMd5 && !Arrays.equals(src.getMd5hash(), dst.getMd5hash())) {
      return false;
    }
    return true;
  }

  private static boolean isNonEmpty(@Nullable byte[] bytes) {
    return bytes != null && bytes.length > 0;
  }

  private static void cancelIfPresent(@Nullable Future<?> future) {
    if (future != null) {
      future.cancel(true);
    }
  }

  private static final class RenameMetadata {
    private final GcsFileInfo srcInfo;
    private final GcsItemId dstId;
    private final boolean dstExists;
    private final boolean dstIsDir;
    private final Optional<GcsItemId> dstConflictingFileId;
    private final Optional<GcsItemId> dstParentOpt;
    private final boolean dstParentExists;

    private RenameMetadata(
        GcsFileInfo srcInfo,
        GcsItemId dstId,
        boolean dstExists,
        boolean dstIsDir,
        Optional<GcsItemId> dstConflictingFileId,
        Optional<GcsItemId> dstParentOpt,
        boolean dstParentExists) {
      this.srcInfo = srcInfo;
      this.dstId = dstId;
      this.dstExists = dstExists;
      this.dstIsDir = dstIsDir;
      this.dstConflictingFileId = dstConflictingFileId;
      this.dstParentOpt = dstParentOpt;
      this.dstParentExists = dstParentExists;
    }
  }

  @VisibleForTesting
  void checkNoFilesConflictingWithDirs(GcsItemId dirItemId) throws IOException {
    ExecutorService statusExecutor = statusExecutorServiceSupplier.get();
    Map<GcsItemId, Future<GcsItemInfo>> futures = new LinkedHashMap<>();
    try {
      for (String dir : getDirs(dirItemId.getObjectName().orElse(""))) {
        GcsItemId fileId =
            GcsItemId.builder().setBucketName(dirItemId.getBucketName()).setObjectName(dir).build();
        futures.put(fileId, statusExecutor.submit(() -> gcsClient.getGcsItemInfo(fileId)));
      }

      // Check results in submission order so that the shallowest conflicting file is reported.
      boolean accessDeniedLogged = false;
      for (Map.Entry<GcsItemId, Future<GcsItemInfo>> entry : futures.entrySet()) {
        try {
          GcsItemInfo fileInfo = getFromFuture(entry.getValue());
          throw GcsExceptionUtil.createFileAlreadyExistsException(
              dirItemId, fileInfo.getItemId(), "Cannot create directory because of existing file");
        } catch (FileNotFoundException ignored) {
          LOG.debug(
              "File '{}' not found, so it does not conflict with directory '{}'.",
              entry.getKey(),
              dirItemId);
        } catch (AccessDeniedException e) {
          // The check is best-effort: a path outside a Credential Access Boundary or a role without
          // storage.objects.get can't be read, and creating the directory still enforces write
          // permission on the target.
          if (!accessDeniedLogged) {
            // Log only the bucket at WARN: object names may contain sensitive data.
            LOG.warn(
                "Skipping the mkdirs conflict check for paths the caller cannot read in bucket {}",
                dirItemId.getBucketName());
            accessDeniedLogged = true;
          }
          LOG.debug("Conflict check access denied for '{}'", entry.getKey(), e);
        }
      }
    } finally {
      for (Future<GcsItemInfo> future : futures.values()) {
        future.cancel(true);
      }
    }
  }

  /**
   * For objects whose name looks like a path (e.g. {@code "a/b/c"} or {@code "a/b/c/"}), returns
   * all parent/intermediate directory paths.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>a/b/c => returns: ["a", "a/b"]
   *   <li>a/b/c/ => returns: ["a", "a/b", "a/b/c"]
   *   <li>a => returns: []
   * </ul>
   *
   * @param objectName Name of an object.
   * @return List of subdirectory like paths.
   */
  @VisibleForTesting
  static ImmutableList<String> getDirs(String objectName) {
    if (isNullOrEmpty(objectName)) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<String> dirs = ImmutableList.builder();
    int index = 0;
    while ((index = objectName.indexOf(PATH_DELIMITER, index)) >= 0) {
      String dir = objectName.substring(0, index);
      if (!dir.isEmpty()) {
        dirs.add(dir);
      }
      index += PATH_DELIMITER.length();
    }
    return dirs.build();
  }

  @VisibleForTesting
  static Telemetry createTelemetry(TelemetryOptions telemetryOptions) {
    ImmutableList.Builder<OperationListener> listeners = ImmutableList.builder();
    telemetryOptions
        .getLoggingTelemetryOptions()
        .filter(LoggingTelemetryOptions::isEnabled)
        .ifPresent(options -> listeners.add(new LoggingTelemetryReporter(options)));
    telemetryOptions
        .getOpenTelemetryOptions()
        .filter(OpenTelemetryOptions::isEnabled)
        .ifPresent(options -> listeners.add(new OpenTelemetryReporter(options)));
    telemetryOptions
        .getCustomTelemetryOptions()
        .ifPresent(options -> listeners.addAll(options.getOperationListeners()));
    return new Telemetry(listeners.build());
  }

  @VisibleForTesting
  Supplier<ExecutorService> initializeReadExecutionServiceSupplier() {
    return Suppliers.memoize(
        () ->
            new ThreadPoolExecutor(
                fileSystemOptions.getReadThreadCount(),
                fileSystemOptions.getReadThreadCount(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder()
                    .setNameFormat("gcs-filesystem-range-pool-%d")
                    .setDaemon(true)
                    .build()));
  }

  @VisibleForTesting
  Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
    return Suppliers.memoize(
        () -> {
          if (fileSystemOptions.isMetadataLookupParallelEnabled()) {
            return createCachedExecutor();
          }
          return new LazyExecutorService();
        });
  }

  private static ExecutorService createCachedExecutor() {

    // Setting corePoolSize equal to maxPoolSize combined with allowCoreThreadTimeOut ensures that
    // incoming tasks immediately spawn new threads up to CACHED_EXECUTOR_MAX_POOL_SIZE before
    // tasks are queued in the LinkedBlockingQueue, while allowing all idle threads to terminate
    // after 30 seconds of inactivity.
    // An unbounded queue is used so that new tasks are not rejected due to queue capacity.
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ CACHED_EXECUTOR_MAX_POOL_SIZE,
            /* maximumPoolSize= */ CACHED_EXECUTOR_MAX_POOL_SIZE,
            /* keepAliveTime= */ CACHED_EXECUTOR_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-filesystem-cached-pool-%d")
                .setDaemon(true)
                .build());
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    // properly shut down the executor, preventing thread leaks.
    service.allowCoreThreadTimeOut(true);
    return service;
  }
}
