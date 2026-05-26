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

package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.FakeGcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GoogleCloudStorageOutputStreamTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_BUCKET = "test-bucket";
  private static final String TEST_OBJECT = "test-object";
  private static final String EMPTY_PREFIX = "";
  private static final String TEST_PAYLOAD = "hello fake world";

  private final BlobInfo blobInfo =
      BlobInfo.newBuilder(BlobId.of(TEST_BUCKET, TEST_OBJECT)).build();
  private final GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();

  private GcsFileSystem fakeFileSystem;
  private GcsItemId itemId;

  @BeforeEach
  void setUp() {
    GcsFileSystemOptions options =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(GcsClientOptions.builder().setProjectId(TEST_PROJECT).build())
            .build();
    fakeFileSystem = new FakeGcsFileSystemImpl(options);
    itemId = GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
  }

  @Test
  void create_initializesWriteSessionAndReturnsStream() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);
    assertThat(stream).isNotNull();
  }

  @Test
  void create_nullFileSystem_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageOutputStream.create(null, blobInfo, writeOptions));

    assertThat(exception).hasMessageThat().isEqualTo("GcsFileSystem shouldn't be null");
  }

  @Test
  void create_nullBlobInfo_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageOutputStream.create(fakeFileSystem, null, writeOptions));

    assertThat(exception).hasMessageThat().isEqualTo("BlobInfo shouldn't be null");
  }

  @Test
  void create_nullWriteOptions_throwsIllegalStateException() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, null));

    assertThat(exception).hasMessageThat().isEqualTo("GcsWriteOptions shouldn't be null");
  }

  @Test
  void write_singleByte_writesToChannel() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);

    stream.write(65); // 'A'
    stream.close();

    byte[] readData = readBackBlob(itemId, 1);
    assertThat(readData).isEqualTo(new byte[] {65});
  }

  @Test
  void write_byteArray_writesToChannel() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);

    byte[] data = new byte[] {1, 2, 3, 4, 5};
    stream.write(data, 1, 3); // Writes {2, 3, 4}
    stream.close();

    byte[] readData = readBackBlob(itemId, 3);
    assertThat(readData).isEqualTo(new byte[] {2, 3, 4});
  }

  @Test
  void write_byteArray_invalidArgs_throwsExceptions() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3};

    assertThrows(NullPointerException.class, () -> stream.write(null, 0, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, -1, 2));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, 0, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> stream.write(data, 2, 2));
  }

  @Test
  void write_byteArray_zeroLength_doesNothing() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);
    byte[] data = new byte[] {1, 2, 3};

    stream.write(data, 0, 0);
    stream.close();

    // Verification: size of object in GCS should be 0 bytes or file should not be created.
    // LocalStorageHelper creates it as a 0-byte file.
    GcsFileInfo fileInfo = fakeFileSystem.getFileInfo(itemId);
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
  }

  @Test
  void close_closesChannel() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);

    stream.close();

    // Trying to write after close should throw IOException
    assertThrows(IOException.class, () -> stream.write(65));
  }

  @Test
  void close_whenChannelAlreadyClosed_isIdempotent() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);
    stream.close();
    stream.close(); // closing again shouldn't throw exception
  }

  @Test
  void getBytesWritten_delegatesToGcsWriteChannel() throws IOException {
    GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeFileSystem, blobInfo, writeOptions);
    stream.write("hello fake world".getBytes(UTF_8));

    assertThat(stream.getBytesWritten()).isEqualTo("hello fake world".length());
  }

  @Test
  void getBytesWritten_nonGcsWriteChannel_returnsZero() throws Exception {
    GcsFileSystem mockFS = mock(GcsFileSystem.class);
    try (Pipe.SinkChannel sink = Pipe.open().sink()) {
      when(mockFS.create(blobInfo, writeOptions)).thenReturn(sink);

      GoogleCloudStorageOutputStream stream =
          GoogleCloudStorageOutputStream.create(mockFS, blobInfo, writeOptions);

      assertThat(stream.getBytesWritten()).isEqualTo(0L);
    }
  }

  @Test
  void write_withFakeGcsFileSystem_writesDataCorrectly() throws IOException {
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(Map.of(), EMPTY_PREFIX);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()))
            .build();

    try (GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeGcsFileSystem, blobInfo, writeOptions)) {
      stream.write(TEST_PAYLOAD.getBytes(UTF_8));
    }

    // Verify the data is present in the fake file system
    GcsFileInfo fileInfo = fakeGcsFileSystem.getFileInfo(itemId);
    assertThat(fileInfo).isNotNull();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(TEST_PAYLOAD.length());

    // Read the data back using GoogleCloudStorageInputStream to verify
    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeGcsFileSystem, itemId)) {
      byte[] buffer = new byte[50];
      int read = inputStream.read(buffer, 0, buffer.length);
      assertThat(new String(buffer, 0, read, UTF_8)).isEqualTo(TEST_PAYLOAD);
    }
  }

  @Test
  void write_multipleChunksAndSingleBytes_withFakeGcsFileSystem() throws IOException {
    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(Map.of(), EMPTY_PREFIX);
    FakeGcsFileSystemImpl fakeGcsFileSystem = new FakeGcsFileSystemImpl(options);
    GcsItemId itemId =
        GcsItemId.builder().setBucketName(TEST_BUCKET).setObjectName(TEST_OBJECT).build();
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()))
            .build();
    int valA = 65; // 'A'
    byte[] chunk1 = "hello".getBytes(UTF_8);
    int valB = 66; // 'B'
    byte[] chunk2 = "world".getBytes(UTF_8);
    int chunk2Offset = 0;
    int chunk2Length = 3; // "wor"

    ByteArrayOutputStream expectedStream = new ByteArrayOutputStream();
    expectedStream.write(valA);
    expectedStream.write(chunk1, 0, chunk1.length);
    expectedStream.write(valB);
    expectedStream.write(chunk2, chunk2Offset, chunk2Length);
    byte[] expectedBytes = expectedStream.toByteArray();

    try (GoogleCloudStorageOutputStream stream =
        GoogleCloudStorageOutputStream.create(fakeGcsFileSystem, blobInfo, writeOptions)) {
      stream.write(valA);
      stream.write(chunk1);
      stream.write(valB);
      stream.write(chunk2, chunk2Offset, chunk2Length);
    }

    // Verify the data is present in the fake file system
    GcsFileInfo fileInfo = fakeGcsFileSystem.getFileInfo(itemId);
    assertThat(fileInfo).isNotNull();
    assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(expectedBytes.length);

    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeGcsFileSystem, itemId)) {
      byte[] buffer = new byte[expectedBytes.length + 10];
      int read = inputStream.read(buffer, 0, buffer.length);
      assertThat(read).isEqualTo(expectedBytes.length);
      byte[] actualBytes = Arrays.copyOf(buffer, read);
      assertThat(actualBytes).isEqualTo(expectedBytes);
    }
  }

  private byte[] readBackBlob(GcsItemId itemId, int length) throws IOException {
    try (GoogleCloudStorageInputStream inputStream =
        GoogleCloudStorageInputStream.create(fakeFileSystem, itemId)) {
      byte[] buffer = new byte[length];
      int read = inputStream.read(buffer, 0, buffer.length);
      return Arrays.copyOf(buffer, read);
    }
  }
}
