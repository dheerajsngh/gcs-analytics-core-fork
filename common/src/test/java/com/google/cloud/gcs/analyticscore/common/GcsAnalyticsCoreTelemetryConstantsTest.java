/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.common;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.common.telemetry.Metric;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GcsAnalyticsCoreTelemetryConstantsTest {

  @Test
  void metric_metadataDurationConstants_haveExpectedNamesAndDurationType() {
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.GET_FILE_INFO_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.get.file.info.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.GET_FILE_INFO_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);

    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.LIST_FILE_INFO_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.list.file.info.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.LIST_FILE_INFO_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);

    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.MKDIRS_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.mkdirs.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.MKDIRS_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);

    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.DELETE_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.delete.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.DELETE_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);

    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.DELETE_FOLDER_RECURSIVE_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.delete.folder.recursive.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.DELETE_FOLDER_RECURSIVE_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);

    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.RENAME_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.rename.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.RENAME_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);

    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.RENAME_FOLDER_DURATION.getName())
        .isEqualTo("gcs.analytics-core.client.rename.folder.duration");
    assertThat(GcsAnalyticsCoreTelemetryConstants.Metric.RENAME_FOLDER_DURATION.getType())
        .isEqualTo(Metric.MetricType.DURATION);
  }

  @Test
  void metric_allConstants_haveUniqueNamesWithClientNamespacePrefix() {
    GcsAnalyticsCoreTelemetryConstants.Metric[] metrics =
        GcsAnalyticsCoreTelemetryConstants.Metric.values();

    Set<String> names =
        Arrays.stream(metrics)
            .map(GcsAnalyticsCoreTelemetryConstants.Metric::getName)
            .collect(Collectors.toSet());

    assertThat(names).hasSize(metrics.length);
    for (GcsAnalyticsCoreTelemetryConstants.Metric metric : metrics) {
      assertThat(metric.getName()).startsWith("gcs.analytics-core.client.");
      assertThat(metric.getType()).isNotNull();
    }
  }

  @Test
  void operation_metadataConstants_arePresent() {
    Set<String> operationNames =
        Arrays.stream(GcsAnalyticsCoreTelemetryConstants.Operation.values())
            .map(Enum::name)
            .collect(Collectors.toSet());

    assertThat(operationNames)
        .containsAtLeast(
            "GET_FILE_INFO",
            "LIST_FILE_INFO",
            "MKDIRS",
            "DELETE",
            "DELETE_FOLDER_RECURSIVE",
            "RENAME",
            "RENAME_FOLDER");
  }
}
