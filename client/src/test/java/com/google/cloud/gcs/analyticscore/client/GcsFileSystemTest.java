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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the default methods of {@link GcsFileSystem}. */
class GcsFileSystemTest {

  private static final URI DIR_URI = URI.create("gs://test-bucket/dir/");
  private static final GcsItemId DIR_ITEM_ID =
      GcsItemId.builder().setBucketName("test-bucket").setObjectName("dir/").build();

  @Test
  void mkdirs_itemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, () -> fileSystem.mkdirs(DIR_ITEM_ID));
    assertThat(e).hasMessageThat().startsWith("mkdirs is not supported by ");
  }

  @Test
  void mkdirs_uri_delegatesToItemIdOverload() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);
    doNothing().when(fileSystem).mkdirs(DIR_ITEM_ID);

    fileSystem.mkdirs(DIR_URI);

    verify(fileSystem).mkdirs(DIR_ITEM_ID);
  }

  @Test
  void mkdirs_uriWithItemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(UnsupportedOperationException.class, () -> fileSystem.mkdirs(DIR_URI));
  }

  @Test
  void mkdirs_nullUri_throwsNullPointerException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(NullPointerException.class, () -> fileSystem.mkdirs((URI) null));
  }

  @Test
  void listFileInfo_itemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> fileSystem.listFileInfo(DIR_ITEM_ID));
    assertThat(e).hasMessageThat().startsWith("listFileInfo is not supported by ");
  }

  @Test
  void listFileInfo_uri_delegatesToItemIdOverload() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);
    List<GcsFileInfo> expected = List.of(GcsFileInfo.createNotFound(DIR_ITEM_ID));
    doReturn(expected).when(fileSystem).listFileInfo(DIR_ITEM_ID);

    List<GcsFileInfo> actual = fileSystem.listFileInfo(DIR_URI);

    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  void listFileInfo_uriWithItemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(UnsupportedOperationException.class, () -> fileSystem.listFileInfo(DIR_URI));
  }

  @Test
  void listFileInfo_nullUri_throwsNullPointerException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(NullPointerException.class, () -> fileSystem.listFileInfo((URI) null));
  }

  @Test
  void delete_itemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> fileSystem.delete(DIR_ITEM_ID, true));
    assertThat(e).hasMessageThat().startsWith("delete is not supported by ");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void delete_uri_delegatesToItemIdOverload(boolean recursive) throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);
    doNothing().when(fileSystem).delete(DIR_ITEM_ID, recursive);

    fileSystem.delete(DIR_URI, recursive);

    verify(fileSystem).delete(DIR_ITEM_ID, recursive);
  }

  @Test
  void delete_percentEncodedUri_decodesPathBeforeDelegating() throws IOException {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);
    URI encodedUri = URI.create("gs://test-bucket/my%20dir/a%25b%3Fc%23d.txt");
    GcsItemId decodedId =
        GcsItemId.builder()
            .setBucketName("test-bucket")
            .setObjectName("my dir/a%b?c#d.txt")
            .build();
    doNothing().when(fileSystem).delete(decodedId, false);

    fileSystem.delete(encodedUri, false);

    verify(fileSystem).delete(decodedId, false);
  }

  @Test
  void delete_uriWithItemIdNotOverridden_throwsUnsupportedOperationException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(UnsupportedOperationException.class, () -> fileSystem.delete(DIR_URI, false));
  }

  @Test
  void delete_nullUri_throwsNullPointerException() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class, CALLS_REAL_METHODS);

    assertThrows(NullPointerException.class, () -> fileSystem.delete((URI) null, false));
  }

  @Test
  void publicSignatures_containNoGuavaTypes() {
    ApiSignatureTestUtils.assertNoGuavaTypesInSignatures(
        GcsFileSystem.class, GcsFileSystemOptions.class, GcsFileSystemOptions.Builder.class);
  }
}
