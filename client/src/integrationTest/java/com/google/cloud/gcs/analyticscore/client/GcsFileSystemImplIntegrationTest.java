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

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;
import com.google.storage.control.v2.DeleteFolderRequest;
import com.google.storage.control.v2.StorageControlClient;
import java.io.FileNotFoundException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

// TODO: Setup buckets and test data as part of setup on place of relying on existing bucket.
class GcsFileSystemImplIntegrationTest {

    private static final String GCS_INTEGRATION_TEST_BUCKET_PROPERTY = "gcs.integration.test.bucket";
    private static final String GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY =
            "gcs.integration.test.hns.bucket";
    private static final String PUBLIC_BUCKET_NAME = "cloud-samples-data";
    private static final String PUBLIC_PARQUET_OBJECT = "bigquery/us-states/us-states.parquet";
    private static final String PUBLIC_CSV_OBJECT = "bigquery/us-states/us-states.csv";
    private static final String PUBLIC_PARQUET_URI_STRING =
            "gs://" + PUBLIC_BUCKET_NAME + "/" + PUBLIC_PARQUET_OBJECT;
    private static final String PUBLIC_CSV_URI_STRING =
            "gs://" + PUBLIC_BUCKET_NAME + "/" + PUBLIC_CSV_OBJECT;
    private static final String PRIVATE_BUCKET_NAME =
            "gcs-connector-private-test-bucket-do-not-delete";
    private static final String PRIVATE_PARQUET_OBJECT = "tpch_customer_1.parquet";
    private static final String PRIVATE_PARQUET_URI_STRING =
            "gs://" + PRIVATE_BUCKET_NAME + "/" + PRIVATE_PARQUET_OBJECT;
    private static final byte[] TEST_FILE_CONTENT =
            "test file content".getBytes(StandardCharsets.UTF_8);

    private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystemImplIntegrationTest.class);

    private Storage storage;
    private List<BlobId> blobsToDelete;
    private List<String> bucketsToDelete;
    private List<String> foldersToDelete;
    private GcsFileSystemImpl gcsFileSystem;

    @BeforeEach
    void setUp() {
        storage = StorageOptions.getDefaultInstance().getService();
        blobsToDelete = new ArrayList<>();
        bucketsToDelete = new ArrayList<>();
        foldersToDelete = new ArrayList<>();
        gcsFileSystem = createFileSystem(GcsClientOptions.builder().build());
    }

    @AfterEach
    void tearDown() {
        try {
            if (gcsFileSystem != null) {
                try {
                    gcsFileSystem.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close gcsFileSystem during cleanup", e);
                }
            }
            if (storage != null) {
                for (BlobId blobId : blobsToDelete) {
                    try {
                        storage.delete(blobId);
                    } catch (Exception e) {
                        LOG.warn("Failed to delete blob {} during cleanup", blobId, e);
                    }
                }
            }
            if (!foldersToDelete.isEmpty()) {
                List<String> sortedFolders = new ArrayList<>(foldersToDelete);
                sortedFolders.sort(
                        Comparator.comparingInt(
                                        (String folder) ->
                                                CharMatcher.is('/')
                                                        .countIn(
                                                                UriUtil.removeTrailingSlash(
                                                                        folder)))
                                .thenComparingInt(String::length)
                                .reversed());
                try (StorageControlClient client = StorageControlClient.create()) {
                    for (String folderResourceName : sortedFolders) {
                        try {
                            client.deleteFolder(
                                    DeleteFolderRequest.newBuilder().setName(folderResourceName).build());
                        } catch (Exception e) {
                            LOG.warn("Failed to delete folder {} during cleanup", folderResourceName, e);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to close StorageControlClient during cleanup", e);
                }
            }
            if (storage != null) {
                for (String bucketName : bucketsToDelete) {
                    try {
                        storage.delete(bucketName);
                    } catch (Exception e) {
                        LOG.warn("Failed to delete bucket {} during cleanup", bucketName, e);
                    }
                }
            }
        } finally {
            blobsToDelete.clear();
            foldersToDelete.clear();
            bucketsToDelete.clear();
        }
    }

    @Test
    void open_publicObject_canReadContent() throws IOException {
        String gcsObject = PUBLIC_CSV_URI_STRING;
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfoObject(URI.create(gcsObject));
        GcsReadOptions readOptions = GcsReadOptions.builder().build();

        try (VectoredSeekableByteChannel channel = gcsFileSystem.open(fileInfo, readOptions)) {
            assertThat(channel.isOpen()).isTrue();
            assertThat(channel.size()).isGreaterThan(0L);

            ByteBuffer buffer = ByteBuffer.allocate(10);
            int bytesRead = channel.read(buffer);

            assertThat(bytesRead).isEqualTo(10);
            // The first line of us-states.csv is "name,post_abbr"
            assertThat(new String(buffer.array(), StandardCharsets.UTF_8)).isEqualTo("name,post_");
        }
    }

    @Test
    void getFileInfo_noCredentialProvided_urlPointsToPublicObject_success() throws IOException {
        String gcsObject = PUBLIC_PARQUET_URI_STRING;

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(gcsObject));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(PUBLIC_PARQUET_OBJECT);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PUBLIC_BUCKET_NAME);
        assertThat(fileInfo.getItemInfo().getSize()).isGreaterThan(0L);
        assertThat(fileInfo.getItemInfo().getContentGeneration().isPresent()).isTrue();
        assertThat(fileInfo.getItemInfo().getCreationTime()).isGreaterThan(0L);
        assertThat(fileInfo.getItemInfo().getModificationTime()).isGreaterThan(0L);
        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
    }

    @Test
    void getFileInfo_noCredentialProvided_urlPointsToPrivateObject_usesApplicationDefaultCredentials()
            throws IOException {
        String object = PRIVATE_PARQUET_URI_STRING;

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(URI.create(object));

        assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(PRIVATE_PARQUET_OBJECT);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PRIVATE_BUCKET_NAME);
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPublicObject_success()
            throws IOException {
        String gcsObject = PUBLIC_PARQUET_URI_STRING;
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();

        try (GcsFileSystemImpl anonFileSystem =
                new GcsFileSystemImpl(NoCredentials.getInstance(), options)) {
            GcsFileInfo fileInfo = anonFileSystem.getFileInfo(URI.create(gcsObject));

            assertThat(fileInfo.getItemInfo().getItemId().isGcsObject()).isTrue();
            assertThat(fileInfo.getItemInfo().getItemId().getObjectName())
                    .hasValue(PUBLIC_PARQUET_OBJECT);
            assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(PUBLIC_BUCKET_NAME);
        }
    }

    @Test
    void getFileInfo_anonymousCredentialProvided_urlPointsToPrivateObject_throws() {
        String object = PRIVATE_PARQUET_URI_STRING;
        GcsFileSystemOptions options =
                GcsFileSystemOptions.builder()
                        .setGcsClientOptions(GcsClientOptions.builder().build())
                        .build();

        try (GcsFileSystemImpl anonFileSystem =
                new GcsFileSystemImpl(NoCredentials.getInstance(), options)) {
            AccessDeniedException exception =
                    assertThrows(
                            AccessDeniedException.class, () -> anonFileSystem.getFileInfo(URI.create(object)));

            assertThat(exception).hasMessageThat().contains("Access denied to object during metadata lookup");
        }
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatBucket_returnsBucketInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI bucketUri = URI.create("gs://" + bucketName);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(bucketUri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).isEmpty();
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getUri()).isEqualTo(bucketUri);
    }

    @Test
    void getFileInfo_nonExistentBucket_throwsFileNotFoundException() {
        URI nonExistentBucketUri = URI.create("gs://non-existent-bucket-" + UUID.randomUUID());
        assertThrows(
                FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentBucketUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatFile_returnsObjectInfoWithAttributes() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete);
        byte[] ownerValue = "alice".getBytes(StandardCharsets.UTF_8);
        storage.create(
                BlobInfo.newBuilder(BlobId.of(bucketName, ctx.objectName))
                        .setMetadata(GcsItemInfo.encodeMetadata(ImmutableMap.of("owner", ownerValue)))
                        .build(),
                TEST_FILE_CONTENT);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_FILE_CONTENT.length);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.objectName);
        assertThat(fileInfo.getUri()).isEqualTo(ctx.uri);
        assertThat(fileInfo.getAttributes().get("owner")).isEqualTo(ownerValue);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatNonExistentObject_throwsFileNotFoundException() {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI nonExistentUri =
                URI.create("gs://" + bucketName + "/non-existent-file-" + UUID.randomUUID() + ".parquet");
        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(nonExistentUri));
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatImplicitDirectory_returnsInferredDirectory(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete);
        writeObject(ctx.itemId, TEST_FILE_CONTENT);
        URI dirUri = URI.create("gs://" + bucketName + "/" + ctx.folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.folderName + "/");
        assertThat(fileInfo.getItemInfo().getContentGeneration()).isEmpty();
        assertThat(fileInfo.getUri()).isEqualTo(dirUri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatPlaceholderDirectory_returnsPlaceholderDirectory(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String folderName = "test-folder-" + UUID.randomUUID();
        createPlaceholderDirectory(bucketName, folderName);
        URI dirUri = URI.create("gs://" + bucketName + "/" + folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(fileInfo.getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.PLACEHOLDER_DIRECTORY);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(folderName + "/");
        assertThat(fileInfo.getItemInfo().getContentGeneration()).isPresent();
        assertThat(fileInfo.getUri()).isEqualTo(dirUri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_flatNonExistentDirectory_throwsFileNotFoundException(boolean trailingSlash) {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI dirUri =
                URI.create("gs://" + bucketName + "/non-existent-folder-" + UUID.randomUUID() + "/");
        URI inputUri = toInputUri(dirUri, trailingSlash);

        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(inputUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsBucket_returnsBucketInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI bucketUri = URI.create("gs://" + bucketName);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(bucketUri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
        assertThat(fileInfo.getItemInfo().getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).isEmpty();
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getUri()).isEqualTo(bucketUri);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFile_returnsObjectInfo() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        writeObject(ctx.itemId, TEST_FILE_CONTENT);

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(ctx.uri);

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.OBJECT);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) TEST_FILE_CONTENT.length);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.objectName);
        assertThat(fileInfo.getUri()).isEqualTo(ctx.uri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsFolder_returnsNativeFolder(boolean trailingSlash) throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        writeObject(ctx.itemId, TEST_FILE_CONTENT);
        URI folderUri = URI.create("gs://" + bucketName + "/" + ctx.folderName + "/");

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(toInputUri(folderUri, trailingSlash));

        assertThat(fileInfo.getItemInfo().getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(ctx.folderName + "/");
        assertThat(fileInfo.getUri()).isEqualTo(folderUri);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void getFileInfo_hnsNonExistentDirectory_throwsFileNotFoundException(boolean trailingSlash) {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI dirUri =
                URI.create("gs://" + bucketName + "/non-existent-folder-" + UUID.randomUUID() + "/");
        URI inputUri = toInputUri(dirUri, trailingSlash);

        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.getFileInfo(inputUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void create_object_canWriteContent() throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        GcsFileInfo fileInfo = gcsFileSystem.getFileInfoObject(ctx.uri);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) content.length);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void create_overwriteDisabled_throwsFileAlreadyExistsException() throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        // We do a preliminary setup write
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }

        GcsWriteOptions noOverwriteOptions = GcsWriteOptions.builder()
                .setOverwriteExisting(false)
                .build();

        FileAlreadyExistsException exception =
                assertThrows(FileAlreadyExistsException.class, () -> {
                    try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, noOverwriteOptions)) {
                        ByteBuffer buffer = ByteBuffer.wrap(content);
                        while (buffer.hasRemaining()) {
                            channel.write(buffer);
                        }
                    }
                });
        assertThat(exception.getFile()).isEqualTo(ctx.uri.toString());
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void create_withParallelCompositeUpload_success() throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        GcsClientOptions clientOptions =
                GcsClientOptions.builder()
                        .setUploadType(GcsClientOptions.UploadType.PARALLEL_COMPOSITE_UPLOAD)
                        .build();
        try (GcsFileSystemImpl pcuFileSystem = createFileSystem(clientOptions)) {
            GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
            byte[] content = "test content".getBytes(StandardCharsets.UTF_8);

            try (WritableByteChannel channel = pcuFileSystem.create(ctx.itemId, writeOptions)) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }

            GcsFileInfo fileInfo = pcuFileSystem.getFileInfoObject(ctx.uri);
            assertThat(fileInfo.getItemInfo().getSize()).isEqualTo((long) content.length);
        }
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void createEmptyObject_directoryMarker_isIdempotentOnSecondCall() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dirObjectName = "test-folder/test-dir-" + UUID.randomUUID() + "/";
        blobsToDelete.add(BlobId.of(bucketName, dirObjectName));
        GcsItemId dirItemId =
                GcsItemId.builder().setBucketName(bucketName).setObjectName(dirObjectName).build();
        GcsClient gcsClient = gcsFileSystem.getGcsClient();

        gcsClient.createEmptyObject(dirItemId);
        GcsItemInfo firstItemInfo = gcsClient.getGcsItemInfo(dirItemId);
        assertThat(firstItemInfo.getSize()).isEqualTo(0L);
        assertThat(firstItemInfo.getContentGeneration()).isPresent();

        // Second call hits HTTP 412 (ifGenerationMatch=0) and verifies the existing 0-byte marker.
        gcsClient.createEmptyObject(dirItemId);
        GcsItemInfo secondItemInfo = gcsClient.getGcsItemInfo(dirItemId);
        assertThat(secondItemInfo.getSize()).isEqualTo(0L);
        assertThat(secondItemInfo.getContentGeneration())
                .isEqualTo(firstItemInfo.getContentGeneration());
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void createEmptyObject_existingNonEmptyObjectWithOverwriteFalse_throwsFileAlreadyExistsException()
            throws IOException {
        TestWriteContext ctx =
                new TestWriteContext(
                        System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY), blobsToDelete);
        byte[] content = "existing-data".getBytes(StandardCharsets.UTF_8);
        writeObject(ctx.itemId, content);
        GcsClient gcsClient = gcsFileSystem.getGcsClient();

        FileAlreadyExistsException exception =
                assertThrows(
                        FileAlreadyExistsException.class,
                        () ->
                                gcsClient.createEmptyObject(
                                        ctx.itemId,
                                        GcsWriteOptions.builder()
                                                .setOverwriteExisting(false)
                                                .build()));

        assertThat(exception.getFile()).isEqualTo(ctx.uri.toString());
        assertThat(gcsClient.getGcsItemInfo(ctx.itemId).getSize())
                .isEqualTo((long) content.length);
    }

    @Test
    void create_nonExistentBucket_throwsFileNotFoundException() {
        TestWriteContext ctx =
                new TestWriteContext("non-existent-bucket-" + UUID.randomUUID(), blobsToDelete);
        GcsWriteOptions writeOptions = GcsWriteOptions.builder().build();
        byte[] content = "test".getBytes(StandardCharsets.UTF_8);

        assertThrows(FileNotFoundException.class, () -> {
            try (WritableByteChannel channel = gcsFileSystem.create(ctx.itemId, writeOptions)) {
                ByteBuffer buffer = ByteBuffer.wrap(content);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            }
        });
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void mkdirs_createsDirectoryMarkerAndIsIdempotent_success() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String parentDirName = "test-mkdirs-" + UUID.randomUUID() + "/";
        String dirObjectName = parentDirName + "subdir/";
        URI parentDirUri = URI.create("gs://" + bucketName + "/" + parentDirName);
        URI dirUri = URI.create("gs://" + bucketName + "/" + dirObjectName);
        blobsToDelete.add(BlobId.of(bucketName, dirObjectName));
        blobsToDelete.add(BlobId.of(bucketName, parentDirName));

        // Verify leaf-only directory marker creation (parent marker object is not created)
        gcsFileSystem.mkdirs(dirUri);
        GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(dirUri);
        assertThat(fileInfo.getItemInfo().getItemId().getObjectName()).hasValue(dirObjectName);
        assertThat(fileInfo.getItemInfo().getSize()).isEqualTo(0L);
        assertThat(storage.get(BlobId.of(bucketName, parentDirName))).isNull();
        assertThat(gcsFileSystem.getFileInfo(parentDirUri).getItemInfo().getItemType())
                .isEqualTo(GcsItemInfo.ItemType.INFERRED_DIRECTORY);

        // Verify calling mkdirs on an existing directory is idempotent
        assertDoesNotThrow(() -> gcsFileSystem.mkdirs(dirUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void mkdirs_withConflictingFile_throwsFileAlreadyExistsException() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete);
        blobsToDelete.add(BlobId.of(bucketName, ctx.objectName + "/subdir/"));
        try (WritableByteChannel channel =
                gcsFileSystem.create(ctx.itemId, GcsWriteOptions.builder().build())) {
            channel.write(ByteBuffer.wrap("file content".getBytes(StandardCharsets.UTF_8)));
        }
        URI subdirUri = URI.create(ctx.uri.toString() + "/subdir");

        FileAlreadyExistsException exception =
                assertThrows(FileAlreadyExistsException.class, () -> gcsFileSystem.mkdirs(subdirUri));
        assertThat(exception.getFile()).isEqualTo(ctx.uri.toString() + "/subdir/");
        assertThat(exception.getOtherFile()).isEqualTo(ctx.uri.toString());
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void mkdirs_createsHnsFolderRecursivelyAndIsIdempotent_success() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String rootFolder = "test-folder-" + UUID.randomUUID();
        String middleFolder = rootFolder + "/nested";
        String leafFolder = middleFolder + "/leaf";
        // Register parent-first so tearDown()'s deepest-first folder deletion is exercised.
        foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + rootFolder);
        foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + middleFolder);
        foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + leafFolder);
        URI leafUri = URI.create("gs://" + bucketName + "/" + leafFolder + "/");
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setHnsApiEnabled(true)
                .build();
        try (GcsFileSystemImpl hnsFileSystem = new GcsFileSystemImpl(options)) {
            hnsFileSystem.mkdirs(leafUri);

            for (String folder : List.of(rootFolder, middleFolder, leafFolder)) {
                URI folderUri = URI.create("gs://" + bucketName + "/" + folder + "/");
                GcsItemInfo folderInfo =
                        hnsFileSystem.getGcsClient().getFolderInfo(UriUtil.getItemIdFromUri(folderUri));
                assertThat(folderInfo.getItemId().getObjectName()).hasValue(folder + "/");
                assertThat(folderInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.NATIVE_FOLDER);
            }

            // Verify calling mkdirs on an existing folder hierarchy is idempotent
            assertDoesNotThrow(() -> hnsFileSystem.mkdirs(leafUri));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void mkdirs_hnsWithConflictingFile_throwsFileAlreadyExistsException() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        TestWriteContext ctx = new TestWriteContext(bucketName, blobsToDelete, foldersToDelete);
        foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + ctx.objectName);
        foldersToDelete.add(
                "projects/_/buckets/" + bucketName + "/folders/" + ctx.objectName + "/subdir");
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setHnsApiEnabled(true)
                .build();
        try (GcsFileSystemImpl hnsFileSystem = new GcsFileSystemImpl(options)) {
            try (WritableByteChannel channel =
                    hnsFileSystem.create(ctx.itemId, GcsWriteOptions.builder().build())) {
                channel.write(ByteBuffer.wrap("file content".getBytes(StandardCharsets.UTF_8)));
            }
            URI subdirUri = URI.create(ctx.uri.toString() + "/subdir");

            FileAlreadyExistsException exception =
                    assertThrows(
                            FileAlreadyExistsException.class,
                            () -> hnsFileSystem.mkdirs(subdirUri));
            assertThat(exception.getFile()).isEqualTo(ctx.uri.toString() + "/subdir/");
            assertThat(exception.getOtherFile()).isEqualTo(ctx.uri.toString());
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
    void mkdirs_newBucket_createsBucket_success() throws IOException {
        String bucketName = "test-mkdirs-bucket-" + UUID.randomUUID();
        URI bucketUri = URI.create("gs://" + bucketName);
        bucketsToDelete.add(bucketName);

        gcsFileSystem.mkdirs(bucketUri);

        GcsItemInfo bucketInfo =
                gcsFileSystem
                        .getGcsClient()
                        .getBucketInfo(GcsItemId.builder().setBucketName(bucketName).build());
        assertThat(bucketInfo.getItemId().getBucketName()).isEqualTo(bucketName);
        assertThat(bucketInfo.getItemType()).isEqualTo(GcsItemInfo.ItemType.BUCKET);
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatDirectory_returnsImmediateChildren(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/");
        createObject(bucketName, dir + "/file.txt");
        createObject(bucketName, dir + "/implicit/nested.txt");
        createObject(bucketName, dir + "/placeholder/");
        createObject(bucketName, dir + "/placeholder/nested.txt");
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(describeListed(listed))
                .containsExactly(
                        dirUri + "file.txt OBJECT",
                        dirUri + "implicit/ INFERRED_DIRECTORY",
                        dirUri + "placeholder/ PLACEHOLDER_DIRECTORY")
                .inOrder();
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatImplicitDirectory_returnsChildren() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/b.txt");
        createObject(bucketName, dir + "/aa.txt");
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(dirUri);

        assertThat(describeListed(listed))
                .containsExactly(dirUri + "b.txt OBJECT", dirUri + "aa.txt OBJECT")
                .inOrder();
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatListedObject_hasObjectMetadata() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/file.txt");
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(dirUri);

        GcsItemInfo listedObject = listed.get(0).getItemInfo();
        assertThat(listedObject.getSize()).isEqualTo((long) TEST_FILE_CONTENT.length);
        assertThat(listedObject.getContentGeneration()).isPresent();
        assertThat(listedObject.getModificationTime()).isGreaterThan(0L);
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatFile_returnsOnlyThatObject() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/file.txt");
        createObject(bucketName, dir + "/other.txt");
        URI fileUri = URI.create("gs://" + bucketName + "/" + dir + "/file.txt");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(fileUri);

        assertThat(describeListed(listed)).containsExactly(fileUri + " OBJECT");
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatFileAndDirectoryWithSameName_returnsOnlyTheFile() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/name");
        createObject(bucketName, dir + "/name/child.txt");
        URI fileUri = URI.create("gs://" + bucketName + "/" + dir + "/name");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(fileUri);

        assertThat(describeListed(listed)).containsExactly(fileUri + " OBJECT");
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatEmptyPlaceholderDirectory_returnsEmptyList(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/");
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(listed).isEmpty();
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatNonExistentPath_throwsFileNotFoundException(boolean trailingSlash) {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        URI dirUri = URI.create("gs://" + bucketName + "/" + newListTestDirectory() + "/");
        URI inputUri = toInputUri(dirUri, trailingSlash);

        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.listFileInfo(inputUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_flatBucket_returnsTopLevelDirectory() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/file.txt");
        URI bucketUri = URI.create("gs://" + bucketName);

        // Lists the top level of the shared test bucket, including what other tests left there.
        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(bucketUri);

        assertThat(describeListed(listed))
                .contains("gs://" + bucketName + "/" + dir + "/ INFERRED_DIRECTORY");
    }

    @Test
    void listFileInfo_nonExistentBucket_throwsFileNotFoundException() {
        URI nonExistentBucketUri = URI.create("gs://non-existent-bucket-" + UUID.randomUUID());

        assertThrows(
                FileNotFoundException.class, () -> gcsFileSystem.listFileInfo(nonExistentBucketUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_TEST_BUCKET_PROPERTY, matches = ".+")
    void list_flatDirectoryInSmallPages_returnsEachChildOnce() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        createObject(bucketName, dir + "/");
        createObject(bucketName, dir + "/a.txt");
        createObject(bucketName, dir + "/b.txt");
        createObject(bucketName, dir + "/c.txt");
        createObject(bucketName, dir + "/implicit/nested.txt");
        createObject(bucketName, dir + "/placeholder/");
        createObject(bucketName, dir + "/placeholder/nested.txt");
        createObject(bucketName, dir + "/z.txt");
        GcsItemId dirId =
                GcsItemId.builder().setBucketName(bucketName).setObjectName(dir + "/").build();
        GcsListOptions twoPerPage = GcsListOptions.LIST_DIRECTORY.toBuilder().setPageSize(2).build();

        DirectoryListing listing =
                DirectoryListing.list(gcsFileSystem.getGcsClient(), dirId, twoPerPage);

        assertThat(listedNames(listing, dir))
                .containsExactly("a.txt", "b.txt", "c.txt", "implicit/", "placeholder/", "z.txt");
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsFolder_returnsObjectsAndChildFolders(boolean trailingSlash)
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir, dir + "/sub", dir + "/empty-child");
        createObject(bucketName, dir + "/file.txt");
        createObject(bucketName, dir + "/sub/nested.txt");
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
        gcsFileSystem.mkdirs(URI.create(dirUri + "empty-child/"));

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(describeListed(listed))
                .containsExactly(
                        dirUri + "sub/ INFERRED_DIRECTORY",
                        dirUri + "file.txt OBJECT",
                        dirUri + "empty-child/ INFERRED_DIRECTORY")
                .inOrder();
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsEmptyFolder_returnsEmptyList(boolean trailingSlash) throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir);
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
        gcsFileSystem.mkdirs(dirUri);

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(toInputUri(dirUri, trailingSlash));

        assertThat(listed).isEmpty();
    }

    @ParameterizedTest(name = "trailingSlash={0}")
    @ValueSource(booleans = {true, false})
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsNonExistentPath_throwsFileNotFoundException(boolean trailingSlash) {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        URI dirUri = URI.create("gs://" + bucketName + "/" + newListTestDirectory() + "/");
        URI inputUri = toInputUri(dirUri, trailingSlash);

        assertThrows(FileNotFoundException.class, () -> gcsFileSystem.listFileInfo(inputUri));
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsFile_returnsOnlyThatObject() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir);
        createObject(bucketName, dir + "/file.txt");
        createObject(bucketName, dir + "/other.txt");
        URI fileUri = URI.create("gs://" + bucketName + "/" + dir + "/file.txt");

        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(fileUri);

        assertThat(describeListed(listed)).containsExactly(fileUri + " OBJECT");
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsBucket_returnsTopLevelEmptyFolder() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir);
        gcsFileSystem.mkdirs(URI.create("gs://" + bucketName + "/" + dir + "/"));
        URI bucketUri = URI.create("gs://" + bucketName);

        // Lists the top level of the shared test bucket, including what other tests left there.
        List<GcsFileInfo> listed = gcsFileSystem.listFileInfo(bucketUri);

        assertThat(describeListed(listed))
                .contains("gs://" + bucketName + "/" + dir + "/ INFERRED_DIRECTORY");
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsBucketListedAsFlat_doesNotReturnEmptyChildFolder() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir, dir + "/empty-child");
        createObject(bucketName, dir + "/file.txt");
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
        gcsFileSystem.mkdirs(URI.create(dirUri + "empty-child/"));
        GcsFileSystemOptions flatOptions =
                GcsFileSystemOptions.builder().setHnsApiEnabled(false).build();

        try (GcsFileSystemImpl flatFileSystem = new GcsFileSystemImpl(flatOptions)) {
            List<GcsFileInfo> listed = flatFileSystem.listFileInfo(dirUri);

            assertThat(describeListed(listed)).containsExactly(dirUri + "file.txt OBJECT");
        }
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void listFileInfo_hnsBucketListedAsFlat_emptyFolderThrowsFileNotFoundException()
            throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir);
        URI dirUri = URI.create("gs://" + bucketName + "/" + dir + "/");
        gcsFileSystem.mkdirs(dirUri);
        GcsFileSystemOptions flatOptions =
                GcsFileSystemOptions.builder().setHnsApiEnabled(false).build();

        try (GcsFileSystemImpl flatFileSystem = new GcsFileSystemImpl(flatOptions)) {
            assertThrows(FileNotFoundException.class, () -> flatFileSystem.listFileInfo(dirUri));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY, matches = ".+")
    void list_hnsFolderInSmallPages_returnsEachChildOnce() throws IOException {
        String bucketName = System.getProperty(GCS_INTEGRATION_HNS_TEST_BUCKET_PROPERTY);
        String dir = newListTestDirectory();
        registerFoldersForCleanup(bucketName, dir, dir + "/sub", dir + "/empty-child");
        createObject(bucketName, dir + "/a.txt");
        createObject(bucketName, dir + "/b.txt");
        createObject(bucketName, dir + "/c.txt");
        createObject(bucketName, dir + "/sub/nested.txt");
        createObject(bucketName, dir + "/z.txt");
        gcsFileSystem.mkdirs(URI.create("gs://" + bucketName + "/" + dir + "/empty-child/"));
        GcsItemId dirId =
                GcsItemId.builder().setBucketName(bucketName).setObjectName(dir + "/").build();
        GcsListOptions twoPerPage =
                GcsListOptions.LIST_DIRECTORY_HNS.toBuilder().setPageSize(2).build();

        DirectoryListing listing =
                DirectoryListing.list(gcsFileSystem.getGcsClient(), dirId, twoPerPage);

        assertThat(listedNames(listing, dir))
                .containsExactly("a.txt", "b.txt", "c.txt", "empty-child/", "sub/", "z.txt");
    }

    private static class TestWriteContext {
        final String folderName;
        final String objectName;
        final URI uri;
        final GcsItemId itemId;

        TestWriteContext(String bucketName, List<BlobId> blobsToDelete) {
            this(bucketName, blobsToDelete, null);
        }

        TestWriteContext(String bucketName, List<BlobId> blobsToDelete, List<String> foldersToDelete) {
            this.folderName = "test-folder-" + UUID.randomUUID();
            this.objectName = folderName + "/test-file-" + UUID.randomUUID() + ".txt";
            this.uri = URI.create("gs://" + bucketName + "/" + objectName);
            this.itemId = GcsItemId.builder()
                    .setBucketName(bucketName)
                    .setObjectName(objectName)
                    .build();
            if (blobsToDelete != null) {
                blobsToDelete.add(BlobId.of(bucketName, objectName));
            }
            if (foldersToDelete != null) {
                foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + folderName);
            }
        }
    }

    /** Returns a unique top-level directory name for a listing test. */
    private static String newListTestDirectory() {
        return "list-file-info-it-" + UUID.randomUUID();
    }

    /** Creates an object and registers it for deletion; a name ending in "/" gets no content. */
    private void createObject(String bucketName, String objectName) {
        BlobId blobId = BlobId.of(bucketName, objectName);
        blobsToDelete.add(blobId);
        storage.create(
                BlobInfo.newBuilder(blobId).build(),
                objectName.endsWith("/") ? new byte[0] : TEST_FILE_CONTENT);
    }

    /** Registers HNS folders, which GCS creates along with their objects, for deletion. */
    private void registerFoldersForCleanup(String bucketName, String... folderNames) {
        for (String folderName : folderNames) {
            foldersToDelete.add("projects/_/buckets/" + bucketName + "/folders/" + folderName);
        }
    }

    /** Returns each listed item as its URI followed by its item type. */
    private static List<String> describeListed(List<GcsFileInfo> listed) {
        List<String> described = new ArrayList<>();
        for (GcsFileInfo fileInfo : listed) {
            described.add(fileInfo.getUri() + " " + fileInfo.getItemInfo().getItemType());
        }
        return described;
    }

    /** Returns the names of the listed children, relative to the listed directory. */
    private static List<String> listedNames(DirectoryListing listing, String dir) {
        List<String> names = new ArrayList<>();
        for (GcsItemInfo child : listing.getChildren()) {
            names.add(child.getItemId().getObjectName().orElse("").substring(dir.length() + 1));
        }
        return names;
    }

    private void writeObject(GcsItemId itemId, byte[] content) throws IOException {
        try (WritableByteChannel channel =
                gcsFileSystem.create(itemId, GcsWriteOptions.builder().build())) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
    }

    /** Returns {@code dirUri} as-is, or with its trailing slash removed. */
    private static URI toInputUri(URI dirUri, boolean trailingSlash) {
        String uri = dirUri.toString();
        return trailingSlash ? dirUri : URI.create(uri.substring(0, uri.length() - 1));
    }

    private void createPlaceholderDirectory(String bucketName, String folderName) {
        BlobId placeholderId = BlobId.of(bucketName, folderName + "/");
        storage.create(BlobInfo.newBuilder(placeholderId).build());
        blobsToDelete.add(placeholderId);
    }

    private GcsFileSystemImpl createFileSystem(GcsClientOptions clientOptions) {
        GcsClientOptions.Builder builder = clientOptions.toBuilder();
        String projectId = System.getProperty("gcs.integration.test.project-id");
        if (projectId != null && !projectId.isEmpty()) {
            builder.setProjectId(projectId);
        }
        GcsFileSystemOptions options = GcsFileSystemOptions.builder()
                .setGcsClientOptions(builder.build())
                .build();
        return new GcsFileSystemImpl(options);
    }
}
