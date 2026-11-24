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

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;

/** Configuration options for the GCS File System. */
@AutoValue
public abstract class GcsFileSystemOptions {

  private static final String READ_THREAD_COUNT_KEY = "analytics-core.read.thread.count";
  private static final String CLIENT_TYPE_KEY = "client.type";
  private static final String HNS_API_ENABLED_KEY = "analytics-core.hierarchical.namespace.enable";
  private static final String METADATA_LOOKUP_PARALLEL_ENABLED_KEY =
      "analytics-core.metadata.lookup.parallel.enable";
  private static final String CREATE_ITEMS_CONFLICT_CHECK_ENABLE_KEY =
      "analytics-core.create.items.conflict.check.enable";
  private static final String AUTO_REPAIR_IMPLICIT_DIRECTORIES_ENABLED_KEY =
      "analytics-core.directory.implicit-repair.enable";
  private static final String MARKER_FILE_PATTERN_KEY =
      "analytics-core.directory.marker-file-pattern";

  /**
   * Cloud Storage client to use.
   *
   * @deprecated Use {@link GcsClientOptions.Protocol} instead.
   */
  @Deprecated
  public enum ClientType {
    HTTP_CLIENT,
    GRPC_CLIENT,
  }

  public abstract int getReadThreadCount();

  /**
   * @deprecated Use {@link GcsClientOptions#getProtocol()} instead.
   */
  @Deprecated
  public abstract ClientType getClientType();

  public abstract GcsClientOptions getGcsClientOptions();

  /** Returns the configuration options for GCS authentication and proxy. */
  public abstract GcsAuthOptions getGcsAuthOptions();

  /** Returns the configuration options for the GCS caching layer. */
  public abstract GcsCacheOptions getGcsCacheOptions();

  public abstract TelemetryOptions getAnalyticsCoreTelemetryOptions();

  public abstract boolean isHnsApiEnabled();

  public abstract boolean isMetadataLookupParallelEnabled();

  /**
   * Returns whether prefix conflict checks are performed before creating directories via {@link
   * GcsFileSystem#mkdirs}.
   *
   * <p>Defaults to {@code true}, issuing {@code O(depth)} object metadata lookups ({@code
   * storage.objects.get}) to fail if a file exists at any path component; paths the caller can't
   * read are skipped. Currently applies only to {@code mkdirs} (not {@link GcsFileSystem#create}).
   */
  public abstract boolean isCreateItemsConflictCheckEnabled();

  /**
   * Returns whether implicit parent directories are automatically repaired by creating an explicit
   * 0-byte marker object after mutating operations (such as {@link GcsFileSystem#rename}) on
   * flat-namespace buckets.
   *
   * <p>Defaults to {@code true}. Ignored on hierarchical namespace (HNS) buckets.
   */
  public abstract boolean isAutoRepairImplicitDirectoriesEnabled();

  @Nullable
  abstract String getRawMarkerFilePattern();

  /**
   * Returns the compiled pattern identifying marker files (for example {@code _SUCCESS}) that must
   * be moved last, after all non-marker objects, during flat-namespace directory renames.
   *
   * <p>When configured, the trimmed regex is compiled as {@code ^(.+/)?(?:<pattern>)$} and matched
   * against the full relative path of each object under the renamed source directory.
   *
   * <p>Defaults to {@link Optional#empty()}. Ignored on hierarchical namespace (HNS) buckets.
   */
  @Memoized
  public Optional<Pattern> getMarkerFilePattern() {
    String rawPattern = getRawMarkerFilePattern();
    return rawPattern == null
        ? Optional.empty()
        : Optional.of(Pattern.compile("^(.+/)?(?:" + rawPattern + ")$"));
  }

  public abstract Builder toBuilder();

  @SuppressWarnings("deprecation")
  public static Builder builder() {
    return new AutoValue_GcsFileSystemOptions.Builder()
        .setReadThreadCount(16)
        .setClientType(ClientType.HTTP_CLIENT)
        .setHnsApiEnabled(true)
        .setMetadataLookupParallelEnabled(true)
        .setCreateItemsConflictCheckEnabled(true)
        .setAutoRepairImplicitDirectoriesEnabled(true)
        .setRawMarkerFilePattern(null)
        .setGcsAuthOptions(GcsAuthOptions.builder().build())
        .setGcsClientOptions(GcsClientOptions.builder().build())
        .setGcsCacheOptions(GcsCacheOptions.builder().build())
        .setAnalyticsCoreTelemetryOptions(TelemetryOptions.builder().build());
  }

  @SuppressWarnings("deprecation")
  public static GcsFileSystemOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsFileSystemOptions.Builder optionsBuilder = GcsFileSystemOptions.builder();
    String readThreadCountKey = prefix + READ_THREAD_COUNT_KEY;
    Optional<String> readThreadCount =
        ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, readThreadCountKey);
    if (readThreadCount.isPresent()) {
      optionsBuilder.setReadThreadCount(
          ConfigurationUtil.safeParseInteger(readThreadCountKey, readThreadCount.get()));
    }
    Optional<String> clientType =
        ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + CLIENT_TYPE_KEY);
    if (clientType.isPresent()) {
      optionsBuilder.setClientType(ClientType.valueOf(clientType.get()));
    }
    Optional<String> hnsApiEnabled =
        ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + HNS_API_ENABLED_KEY);
    if (hnsApiEnabled.isPresent()) {
      optionsBuilder.setHnsApiEnabled(Boolean.parseBoolean(hnsApiEnabled.get()));
    }
    Optional<String> metadataLookupParallelEnabled =
        ConfigurationUtil.getTrimmedValue(
            analyticsCoreOptions, prefix + METADATA_LOOKUP_PARALLEL_ENABLED_KEY);
    if (metadataLookupParallelEnabled.isPresent()) {
      optionsBuilder.setMetadataLookupParallelEnabled(
          Boolean.parseBoolean(metadataLookupParallelEnabled.get()));
    }
    Optional<String> createItemsConflictCheckEnabled =
        ConfigurationUtil.getTrimmedValue(
            analyticsCoreOptions, prefix + CREATE_ITEMS_CONFLICT_CHECK_ENABLE_KEY);
    if (createItemsConflictCheckEnabled.isPresent()) {
      optionsBuilder.setCreateItemsConflictCheckEnabled(
          Boolean.parseBoolean(createItemsConflictCheckEnabled.get()));
    }
    Optional<String> autoRepairImplicitDirectoriesEnabled =
        ConfigurationUtil.getTrimmedValue(
            analyticsCoreOptions, prefix + AUTO_REPAIR_IMPLICIT_DIRECTORIES_ENABLED_KEY);
    if (autoRepairImplicitDirectoriesEnabled.isPresent()) {
      optionsBuilder.setAutoRepairImplicitDirectoriesEnabled(
          Boolean.parseBoolean(autoRepairImplicitDirectoriesEnabled.get()));
    }
    Optional<String> markerFilePattern =
        ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, prefix + MARKER_FILE_PATTERN_KEY);
    markerFilePattern.ifPresent(optionsBuilder::setMarkerFilePattern);

    optionsBuilder.setGcsAuthOptions(
        GcsAuthOptions.createFromOptions(analyticsCoreOptions, prefix));
    optionsBuilder.setGcsClientOptions(
        GcsClientOptions.createFromOptions(analyticsCoreOptions, prefix));
    optionsBuilder.setGcsCacheOptions(
        GcsCacheOptions.createFromOptions(analyticsCoreOptions, prefix));
    // Analytics Core telemetry options are prefixed with "analytics-core."
    optionsBuilder.setAnalyticsCoreTelemetryOptions(
        TelemetryOptions.createFromOptions(analyticsCoreOptions, prefix + "analytics-core."));

    try {
      return optionsBuilder.build();
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid regular expression for "
              + prefix
              + MARKER_FILE_PATTERN_KEY
              + ": "
              + markerFilePattern.orElse(""),
          e);
    }
  }

  /** Builder for {@link GcsFileSystemOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * @deprecated Use {@link GcsClientOptions.Builder#setProtocol(GcsClientOptions.Protocol)}
     *     instead.
     */
    @Deprecated
    public abstract Builder setClientType(ClientType clientType);

    public abstract Builder setReadThreadCount(int readThreadCount);

    public abstract Builder setHnsApiEnabled(boolean isHnsApiEnabled);

    public abstract Builder setCreateItemsConflictCheckEnabled(
        boolean isCreateItemsConflictCheckEnabled);

    public abstract Builder setAutoRepairImplicitDirectoriesEnabled(
        boolean isAutoRepairImplicitDirectoriesEnabled);

    abstract Builder setRawMarkerFilePattern(@Nullable String rawMarkerFilePattern);

    /**
     * Sets the regular expression identifying marker files (for example {@code _SUCCESS}) that must
     * be moved last during flat-namespace directory renames, or clears it when {@code
     * markerFilePattern} is {@code null} or blank.
     *
     * <p>The trimmed regex is compiled as {@code ^(.+/)?(?:<pattern>)$} in {@link #build()} and
     * matched against the full relative path of each object under the renamed source directory.
     */
    public Builder setMarkerFilePattern(@Nullable String markerFilePattern) {
      if (markerFilePattern == null) {
        return setRawMarkerFilePattern(null);
      }
      String trimmed = markerFilePattern.trim();
      return setRawMarkerFilePattern(trimmed.isEmpty() ? null : trimmed);
    }

    /** Clears any configured marker file pattern. */
    public Builder clearMarkerFilePattern() {
      return setRawMarkerFilePattern(null);
    }

    public abstract Builder setMetadataLookupParallelEnabled(
        boolean isMetadataLookupParallelEnabled);

    public abstract Builder setGcsAuthOptions(GcsAuthOptions gcsAuthOptions);

    public abstract Builder setGcsClientOptions(GcsClientOptions gcsClientOptions);

    /** Sets the configuration options for the GCS caching layer. */
    public abstract Builder setGcsCacheOptions(GcsCacheOptions gcsCacheOptions);

    public abstract Builder setAnalyticsCoreTelemetryOptions(TelemetryOptions telemetryOptions);

    abstract GcsFileSystemOptions autoBuild();

    /**
     * Builds the {@link GcsFileSystemOptions} instance.
     *
     * @throws PatternSyntaxException if a configured marker file pattern is not a valid regular
     *     expression
     */
    public GcsFileSystemOptions build() {
      GcsFileSystemOptions options = autoBuild();
      // Eagerly compile and cache the marker file pattern so invalid regexes fail fast at build
      // time.
      options.getMarkerFilePattern();
      return options;
    }
  }
}
