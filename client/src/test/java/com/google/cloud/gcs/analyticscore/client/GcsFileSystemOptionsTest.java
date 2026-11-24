/*
 * Copyright 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GcsFileSystemOptionsTest {

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.project-id", "test-project",
            "fs.gs.client.protocol", "GRPC",
            "fs.gs.analytics-core.read.thread.count", "32",
            "fs.gs.analytics-core.hierarchical.namespace.enable", "true",
            "fs.gs.analytics-core.create.items.conflict.check.enable", "false");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().get()).isEqualTo("test-project");
    assertThat(options.getGcsClientOptions().getProtocol())
        .isEqualTo(GcsClientOptions.Protocol.GRPC);
    assertThat(options.getReadThreadCount()).isEqualTo(32);
    assertThat(options.isHnsApiEnabled()).isTrue();
    assertThat(options.isCreateItemsConflictCheckEnabled()).isFalse();
  }

  @Test
  void createFromOptions_cacheProperties_createsCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.analytics-core.footer.cache.enabled",
            "false",
            "fs.gs.analytics-core.footer.cache.max-size-bytes",
            String.valueOf(500 * MB),
            "fs.gs.analytics-core.small-file.cache.enabled",
            "true",
            "fs.gs.analytics-core.small-file.cache.max-size-bytes",
            String.valueOf(200 * MB));

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    GcsCacheOptions cacheOptions = options.getGcsCacheOptions();
    assertThat(cacheOptions.isFooterCacheEnabled()).isFalse();
    assertThat(cacheOptions.getFooterCacheMaxSizeBytes()).isEqualTo(500 * MB);
    assertThat(cacheOptions.isSmallObjectCacheEnabled()).isTrue();
    assertThat(cacheOptions.getSmallObjectCacheMaxSizeBytes()).isEqualTo(200 * MB);
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().isEmpty()).isTrue();
    assertThat(options.getGcsClientOptions().getProtocol())
        .isEqualTo(GcsClientOptions.Protocol.HTTP);
    assertThat(options.getReadThreadCount()).isEqualTo(16);
    assertThat(options.isHnsApiEnabled()).isTrue();
    assertThat(options.isCreateItemsConflictCheckEnabled()).isTrue();
    assertThat(options.isAutoRepairImplicitDirectoriesEnabled()).isTrue();
    assertThat(options.getMarkerFilePattern()).isEmpty();

    GcsCacheOptions cacheOptions = options.getGcsCacheOptions();
    assertThat(cacheOptions.isFooterCacheEnabled()).isFalse();
    assertThat(cacheOptions.getFooterCacheMaxSizeBytes()).isEqualTo(100 * MB);
    assertThat(cacheOptions.isSmallObjectCacheEnabled()).isFalse();
    assertThat(cacheOptions.getSmallObjectCacheMaxSizeBytes()).isEqualTo(200 * MB);
  }

  @Test
  void createFromOptions_withBidiProtocol_shouldParseCorrectly() {
    ImmutableMap<String, String> properties = ImmutableMap.of("fs.gs.client.protocol", "BIDI");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProtocol())
        .isEqualTo(GcsClientOptions.Protocol.BIDI);
  }

  @Test
  void createFromOptions_withMetadataLookupParallelEnabledFalse_createsCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("fs.gs.analytics-core.metadata.lookup.parallel.enable", "false");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.isMetadataLookupParallelEnabled()).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  void createFromOptions_withClientType_setsClientType() {
    ImmutableMap<String, String> properties = ImmutableMap.of("fs.gs.client.type", "GRPC_CLIENT");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.GRPC_CLIENT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"INVALID_CLIENT", "grpc_client", "GRPC-CLIENT"})
  void createFromOptions_withInvalidClientType_throwsIllegalArgumentException(
      String invalidClientType) {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("fs.gs.client.type", invalidClientType);

    assertThrows(
        IllegalArgumentException.class,
        () -> GcsFileSystemOptions.createFromOptions(properties, "fs.gs."));
  }

  @Test
  void createFromOptions_readThreadCountOverflowsInt_throwsIllegalArgumentException() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("fs.gs.analytics-core.read.thread.count", "2147483648");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsFileSystemOptions.createFromOptions(properties, "fs.gs."));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "fs.gs.analytics-core.read.thread.count=2147483648 cannot be greater than"
                + " Integer.MAX_VALUE (2147483647)");
  }

  @Test
  @SuppressWarnings("deprecation")
  void createFromOptions_withBlankAndNullValues_retainsDefaults() {
    // Arrange
    Map<String, String> properties = new HashMap<>();
    properties.put("fs.gs.analytics-core.read.thread.count", "   ");
    properties.put("fs.gs.client.type", "");
    properties.put("fs.gs.analytics-core.hierarchical.namespace.enable", null);
    properties.put("fs.gs.analytics-core.metadata.lookup.parallel.enable", "  ");
    properties.put("fs.gs.analytics-core.create.items.conflict.check.enable", "");

    // Act
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    // Assert
    assertThat(options.getReadThreadCount()).isEqualTo(16);
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.HTTP_CLIENT);
    assertThat(options.isHnsApiEnabled()).isTrue();
    assertThat(options.isMetadataLookupParallelEnabled()).isTrue();
    assertThat(options.isCreateItemsConflictCheckEnabled()).isTrue();
  }

  @Test
  void createFromOptions_withWhitespacePaddedTrueAndThreadCount_parsesTrimmedValues() {
    // Arrange
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.analytics-core.read.thread.count", " 32 ",
            "fs.gs.analytics-core.hierarchical.namespace.enable", " true ",
            "fs.gs.analytics-core.metadata.lookup.parallel.enable", " TRUE ",
            "fs.gs.analytics-core.create.items.conflict.check.enable", " true ");

    // Act
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    // Assert
    assertThat(options.getReadThreadCount()).isEqualTo(32);
    assertThat(options.isHnsApiEnabled()).isTrue();
    assertThat(options.isMetadataLookupParallelEnabled()).isTrue();
    assertThat(options.isCreateItemsConflictCheckEnabled()).isTrue();
  }

  @Test
  void createFromOptions_withWhitespacePaddedFalseBooleans_disablesFeatures() {
    // Arrange
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.analytics-core.hierarchical.namespace.enable", " false ",
            "fs.gs.analytics-core.metadata.lookup.parallel.enable", " FALSE ",
            "fs.gs.analytics-core.create.items.conflict.check.enable", " false ");

    // Act
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    // Assert
    assertThat(options.isHnsApiEnabled()).isFalse();
    assertThat(options.isMetadataLookupParallelEnabled()).isFalse();
    assertThat(options.isCreateItemsConflictCheckEnabled()).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  void createFromOptions_withWhitespacePaddedClientType_setsClientType() {
    ImmutableMap<String, String> properties = ImmutableMap.of("fs.gs.client.type", " GRPC_CLIENT ");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.GRPC_CLIENT);
  }

  @Test
  @SuppressWarnings("deprecation")
  void createFromOptions_withBlankClientType_retainsDefault() {
    // Arrange
    ImmutableMap<String, String> properties = ImmutableMap.of("fs.gs.client.type", "   ");

    // Act
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    // Assert
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.HTTP_CLIENT);
  }

  @Test
  void createFromOptions_directoryRenameOptions_parsesValues() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.analytics-core.directory.implicit-repair.enable", " false ",
            "fs.gs.analytics-core.directory.marker-file-pattern", " _SUCCESS ");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.isAutoRepairImplicitDirectoriesEnabled()).isFalse();
    assertThat(options.getMarkerFilePattern()).isPresent();
    Pattern pattern = options.getMarkerFilePattern().get();
    assertThat(pattern.pattern()).isEqualTo("^(.+/)?(?:_SUCCESS)$");
    assertThat(pattern.matcher("_SUCCESS").matches()).isTrue();
    assertThat(pattern.matcher("sub/dir/_SUCCESS").matches()).isTrue();
    assertThat(pattern.matcher("_SUCCESS_extra").matches()).isFalse();
    assertThat(pattern.matcher("part-00000.parquet").matches()).isFalse();
  }

  @Test
  void createFromOptions_blankAndNullDirectoryRenameOptions_retainsDefaults() {
    Map<String, String> properties = new HashMap<>();
    properties.put("fs.gs.analytics-core.directory.implicit-repair.enable", "   ");
    properties.put("fs.gs.analytics-core.directory.marker-file-pattern", "   ");

    GcsFileSystemOptions blankOptions =
        GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(blankOptions.isAutoRepairImplicitDirectoriesEnabled()).isTrue();
    assertThat(blankOptions.getMarkerFilePattern()).isEmpty();

    properties.put("fs.gs.analytics-core.directory.implicit-repair.enable", null);
    properties.put("fs.gs.analytics-core.directory.marker-file-pattern", null);
    GcsFileSystemOptions nullOptions = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(nullOptions.isAutoRepairImplicitDirectoriesEnabled()).isTrue();
    assertThat(nullOptions.getMarkerFilePattern()).isEmpty();
  }

  @Test
  void createFromOptions_invalidMarkerFilePattern_throwsIllegalArgumentExceptionNamingConfigKey() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("fs.gs.analytics-core.directory.marker-file-pattern", "[unclosed");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsFileSystemOptions.createFromOptions(properties, "fs.gs."));

    assertThat(exception)
        .hasMessageThat()
        .contains("fs.gs.analytics-core.directory.marker-file-pattern");
    assertThat(exception).hasCauseThat().isInstanceOf(PatternSyntaxException.class);
  }

  @Test
  void builder_setMarkerFilePattern_regexString_compilesAnchoredPattern() {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder().setMarkerFilePattern("_SUCCESS|\\.done").build();

    assertThat(options.getMarkerFilePattern()).isPresent();
    Pattern pattern = options.getMarkerFilePattern().get();
    assertThat(pattern.pattern()).isEqualTo("^(.+/)?(?:_SUCCESS|\\.done)$");
    assertThat(pattern.matcher("a/b/_SUCCESS").matches()).isTrue();
    assertThat(pattern.matcher(".done").matches()).isTrue();
    assertThat(pattern.matcher("a/b/.done").matches()).isTrue();
    assertThat(pattern.matcher("a/b/file.done").matches()).isFalse();
    assertThat(pattern.matcher("_SUCCESS.bak").matches()).isFalse();
  }

  @Test
  void builder_setMarkerFilePattern_nullString_clearsPattern() {
    GcsFileSystemOptions initial =
        GcsFileSystemOptions.builder().setMarkerFilePattern("_SUCCESS").build();

    GcsFileSystemOptions cleared = initial.toBuilder().setMarkerFilePattern(null).build();

    assertThat(cleared.getMarkerFilePattern()).isEmpty();
  }

  @Test
  void builder_setMarkerFilePattern_blankString_clearsPattern() {
    GcsFileSystemOptions initial =
        GcsFileSystemOptions.builder().setMarkerFilePattern("_SUCCESS").build();

    GcsFileSystemOptions cleared = initial.toBuilder().setMarkerFilePattern("   ").build();

    assertThat(cleared.getMarkerFilePattern()).isEmpty();
  }

  @Test
  void builder_clearMarkerFilePattern_clearsPattern() {
    GcsFileSystemOptions initial =
        GcsFileSystemOptions.builder().setMarkerFilePattern("_SUCCESS").build();

    GcsFileSystemOptions cleared = initial.toBuilder().clearMarkerFilePattern().build();

    assertThat(cleared.getMarkerFilePattern()).isEmpty();
  }

  @Test
  void builder_setMarkerFilePattern_invalidRegex_throwsPatternSyntaxExceptionOnBuild() {
    GcsFileSystemOptions.Builder builder =
        GcsFileSystemOptions.builder().setMarkerFilePattern("[unclosed");

    assertThrows(PatternSyntaxException.class, builder::build);
  }

  @Test
  void equalsAndHashCode_sameMarkerFilePattern_areEqualByValue() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("fs.gs.analytics-core.directory.marker-file-pattern", "_SUCCESS");

    GcsFileSystemOptions fromMap1 = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");
    GcsFileSystemOptions fromMap2 = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");
    GcsFileSystemOptions fromBuilderPadded =
        GcsFileSystemOptions.builder().setMarkerFilePattern("  _SUCCESS  ").build();
    GcsFileSystemOptions different =
        GcsFileSystemOptions.builder().setMarkerFilePattern("_DONE").build();

    assertThat(fromMap1).isEqualTo(fromMap2);
    assertThat(fromMap1.hashCode()).isEqualTo(fromMap2.hashCode());
    assertThat(fromMap1).isEqualTo(fromBuilderPadded);
    assertThat(fromMap1.hashCode()).isEqualTo(fromBuilderPadded.hashCode());
    assertThat(fromMap1).isNotEqualTo(different);
    assertThat(fromMap1.getMarkerFilePattern().orElseThrow())
        .isSameInstanceAs(fromMap1.getMarkerFilePattern().orElseThrow());
  }

  @Test
  void signatures_doNotExposeGuavaTypes() {
    GcsFileSystemOptions built = GcsFileSystemOptions.builder().build();
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsFileSystem.class,
        GcsFileSystemOptions.class,
        GcsFileSystemOptions.Builder.class,
        built.getClass(),
        built.getClass().getSuperclass(),
        GcsFileSystemOptions.builder().getClass());
  }
}
