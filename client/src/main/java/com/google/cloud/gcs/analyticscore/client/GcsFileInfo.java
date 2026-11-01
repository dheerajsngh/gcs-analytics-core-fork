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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Contains information about a GCS file.
 *
 * <p>Note: This class wraps GcsItemInfo, adds file system specific information and hides
 * bucket/object specific information.
 */
@AutoValue
public abstract class GcsFileInfo {

  public static final URI GCS_ROOT_URI = URI.create("gs:/");

  public static final GcsFileInfo ROOT_INFO =
      builder()
          .setItemInfo(GcsItemInfo.ROOT_INFO)
          .setUri(GCS_ROOT_URI)
          .setAttributes(ImmutableMap.of())
          .build();

  public abstract GcsItemInfo getItemInfo();

  /** Gets the path of this file or directory. */
  public abstract URI getUri();

  /**
   * Retrieve file attributes for this file.
   *
   * @return A map of file attributes
   */
  public abstract ImmutableMap<String, byte[]> getAttributes();

  /** Indicates whether this file or directory exists. */
  public boolean exists() {
    return getItemInfo().exists();
  }

  /** Indicates whether this file or directory is a directory. */
  public boolean isDirectory() {
    return getItemInfo().isDirectory();
  }

  /** Creates a {@link GcsFileInfo} representing a non-existent item at the given {@code uri}. */
  public static GcsFileInfo createNotFound(URI uri) {
    GcsItemId itemId = toItemId(uri);
    return builder()
        .setItemInfo(GcsItemInfo.createNotFound(itemId))
        .setUri(uri)
        .setAttributes(ImmutableMap.of())
        .build();
  }

  /**
   * Creates a {@link GcsFileInfo} representing a non-existent item with the given {@code itemId}.
   */
  public static GcsFileInfo createNotFound(GcsItemId itemId) {
    URI uri = toUri(itemId);
    return builder()
        .setItemInfo(GcsItemInfo.createNotFound(itemId))
        .setUri(uri)
        .setAttributes(ImmutableMap.of())
        .build();
  }

  private static URI toUri(GcsItemId itemId) {
    checkArgument(itemId != null, "itemId should not be null");
    if (itemId.isRoot()) {
      return GCS_ROOT_URI;
    }
    checkArgument(
        !itemId.getBucketName().contains("/"), "Invalid GCS itemId bucket name: %s", itemId);
    try {
      if (itemId.isBucket()) {
        return new URI("gs", itemId.getBucketName(), null, null, null);
      }
      checkArgument(itemId.isGcsObject(), "Invalid GCS itemId: %s", itemId);
      return new URI(
          "gs", itemId.getBucketName(), "/" + itemId.getObjectName().orElse(""), null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid GCS itemId: " + itemId, e);
    }
  }

  private static GcsItemId toItemId(URI uri) {
    checkArgument(uri != null, "uri should not be null");
    checkArgument("gs".equalsIgnoreCase(uri.getScheme()), "Invalid GCS URI: %s", uri);
    checkArgument(!uri.isOpaque(), "Invalid GCS URI: %s", uri);
    checkArgument(
        uri.getRawQuery() == null && uri.getRawFragment() == null,
        "GCS URI must not contain query or fragment: %s",
        uri);

    String authority = uri.getAuthority() != null ? uri.getAuthority() : uri.getHost();
    if (isNullOrEmpty(authority)) {
      String rawSchemeSpecificPart = uri.getRawSchemeSpecificPart();
      checkArgument(
          "/".equals(rawSchemeSpecificPart) || "//".equals(rawSchemeSpecificPart),
          "Invalid GCS URI: %s",
          uri);
      return GcsItemId.ROOT;
    }

    checkArgument(!authority.contains("/"), "Invalid GCS URI: %s", uri);

    String rawPath = uri.getRawPath();
    checkArgument(
        rawPath == null || !rawPath.contains("//"),
        "GCS path must not have consecutive '/' characters: %s",
        uri);

    String decodedPath = uri.getPath();
    if (isNullOrEmpty(decodedPath) || "/".equals(decodedPath)) {
      return GcsItemId.builder().setBucketName(authority).build();
    }

    checkArgument(decodedPath.startsWith("/"), "Invalid GCS URI: %s", uri);
    return GcsItemId.builder()
        .setBucketName(authority)
        .setObjectName(decodedPath.substring(1))
        .build();
  }

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsFileInfo.Builder();
  }

  /** Builder for {@link GcsFileInfo}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setItemInfo(GcsItemInfo itemInfo);

    public abstract Builder setUri(URI uri);

    public abstract Builder setAttributes(Map<String, byte[]> attributes);

    public abstract GcsFileInfo build();
  }
}
