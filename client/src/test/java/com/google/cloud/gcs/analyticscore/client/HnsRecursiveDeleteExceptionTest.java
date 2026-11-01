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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class HnsRecursiveDeleteExceptionTest {

  private static final GcsItemId FOLDER_ID =
      GcsItemId.builder().setBucketName("hns-bucket").setObjectName("dir/subdir/").build();

  @Test
  void constructorWithOperationName_populatesGettersMessageAndCause() {
    IOException cause = new IOException("underlying LRO failure");

    HnsRecursiveDeleteException exception =
        new HnsRecursiveDeleteException(
            FOLDER_ID,
            "projects/_/buckets/hns-bucket/operations/op-123",
            "Recursive delete failed for gs://hns-bucket/dir/subdir/",
            cause);

    assertThat(exception).isInstanceOf(IOException.class);
    assertThat(exception.getFolderId()).isEqualTo(FOLDER_ID);
    assertThat(exception.getOperationName())
        .hasValue("projects/_/buckets/hns-bucket/operations/op-123");
    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("Recursive delete failed for gs://hns-bucket/dir/subdir/");
    assertThat(exception).hasCauseThat().isSameInstanceAs(cause);
  }

  @Test
  void constructorWithNullOperationName_returnsEmptyOptional() {
    HnsRecursiveDeleteException exception =
        new HnsRecursiveDeleteException(FOLDER_ID, null, "Timed out before LRO started", null);

    assertThat(exception.getFolderId()).isEqualTo(FOLDER_ID);
    assertThat(exception.getOperationName()).isEmpty();
    assertThat(exception).hasMessageThat().isEqualTo("Timed out before LRO started");
    assertThat(exception.getCause()).isNull();
  }

  @Test
  void threeArgConstructor_setsEmptyOperationNameAndPreservesCause() {
    RuntimeException cause = new RuntimeException("transport error");

    HnsRecursiveDeleteException exception =
        new HnsRecursiveDeleteException(FOLDER_ID, "Delete failed", cause);

    assertThat(exception.getFolderId()).isEqualTo(FOLDER_ID);
    assertThat(exception.getOperationName()).isEmpty();
    assertThat(exception).hasMessageThat().isEqualTo("Delete failed");
    assertThat(exception).hasCauseThat().isSameInstanceAs(cause);
  }

  @Test
  void constructors_whenFolderIdIsNull_throwNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new HnsRecursiveDeleteException(null, "operations/op-1", "msg", null));
    assertThrows(
        NullPointerException.class, () -> new HnsRecursiveDeleteException(null, "msg", null));
  }

  @Test
  void serialization_roundTripsFolderIdOperationNameCauseAndSuppressed() throws Exception {
    GcsItemId versionedFolderId =
        GcsItemId.builder()
            .setBucketName("hns-bucket")
            .setObjectName("dir/subdir/")
            .setContentGeneration(99L)
            .build();
    IOException cause = new IOException("deadline exceeded");
    IOException suppressed = new IOException("secondary error");
    HnsRecursiveDeleteException original =
        new HnsRecursiveDeleteException(
            versionedFolderId, "operations/op-999", "Partial recursive delete", cause);
    original.addSuppressed(suppressed);

    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
      bytes = baos.toByteArray();
    }

    HnsRecursiveDeleteException restored;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      restored = (HnsRecursiveDeleteException) ois.readObject();
    }

    assertThat(restored.getFolderId()).isEqualTo(versionedFolderId);
    assertThat(restored.getOperationName()).hasValue("operations/op-999");
    assertThat(restored).hasMessageThat().isEqualTo("Partial recursive delete");
    assertThat(restored.getCause()).hasMessageThat().isEqualTo("deadline exceeded");
    assertThat(restored.getSuppressed()).hasLength(1);
    assertThat(restored.getSuppressed()[0]).hasMessageThat().isEqualTo("secondary error");
  }

  @Test
  void folderIdField_isDeclaredTransientForSerializableCompliance() throws Exception {
    Field folderIdField = HnsRecursiveDeleteException.class.getDeclaredField("folderId");

    assertThat(Modifier.isTransient(folderIdField.getModifiers())).isTrue();
  }

  @Test
  void serializationProxy_whenCorruptedStreamHasNullFields_validatesAndGuardsAgainstNpe()
      throws Exception {
    HnsRecursiveDeleteException valid =
        new HnsRecursiveDeleteException(FOLDER_ID, "operations/op-1", "Delete failed", null);
    Method writeReplace = HnsRecursiveDeleteException.class.getDeclaredMethod("writeReplace");
    writeReplace.setAccessible(true);

    // 1. Null bucketName in stream -> throws InvalidObjectException instead of NPE.
    Object proxyWithNullBucket = writeReplace.invoke(valid);
    setPrivateField(proxyWithNullBucket, "bucketName", null);
    InvalidObjectException nullBucketEx =
        assertThrows(InvalidObjectException.class, () -> roundTripRawObject(proxyWithNullBucket));
    assertThat(nullBucketEx).hasMessageThat().contains("bucketName");

    // 2. Null stackTrace and suppressed arrays in stream -> guarded without throwing NPE.
    Object proxyWithNullArrays = writeReplace.invoke(valid);
    setPrivateField(proxyWithNullArrays, "stackTrace", null);
    setPrivateField(proxyWithNullArrays, "suppressed", null);
    HnsRecursiveDeleteException restoredWithNullArrays =
        (HnsRecursiveDeleteException) roundTripRawObject(proxyWithNullArrays);
    assertThat(restoredWithNullArrays.getFolderId()).isEqualTo(FOLDER_ID);
    assertThat(restoredWithNullArrays.getOperationName()).hasValue("operations/op-1");
    assertThat(restoredWithNullArrays.getSuppressed()).isEmpty();

    // 3. Null element inside stackTrace or suppressed -> throws InvalidObjectException instead of
    // NPE.
    Object proxyWithNullStackElement = writeReplace.invoke(valid);
    setPrivateField(proxyWithNullStackElement, "stackTrace", new StackTraceElement[] {null});
    assertThrows(InvalidObjectException.class, () -> roundTripRawObject(proxyWithNullStackElement));

    Object proxyWithNullSuppressedElement = writeReplace.invoke(valid);
    setPrivateField(proxyWithNullSuppressedElement, "suppressed", new Throwable[] {null});
    assertThrows(
        InvalidObjectException.class, () -> roundTripRawObject(proxyWithNullSuppressedElement));
  }

  @Test
  void serialization_edgeCaseItemIds_roundTripsRootBucketOnlyAndSpecialUriCharacters()
      throws Exception {
    // 1. GcsItemId.ROOT (empty bucketName and empty objectName).
    HnsRecursiveDeleteException rootException =
        new HnsRecursiveDeleteException(GcsItemId.ROOT, null, "Root delete failed", null);
    HnsRecursiveDeleteException restoredRoot =
        (HnsRecursiveDeleteException) roundTripRawObject(rootException);
    assertThat(restoredRoot.getFolderId()).isEqualTo(GcsItemId.ROOT);
    assertThat(restoredRoot.getFolderId().isRoot()).isTrue();
    assertThat(restoredRoot.getOperationName()).isEmpty();

    // 2. Bucket-only GcsItemId (Optional.empty() objectName).
    GcsItemId bucketOnlyId = GcsItemId.builder().setBucketName("b").build();
    HnsRecursiveDeleteException bucketOnlyException =
        new HnsRecursiveDeleteException(
            bucketOnlyId, "operations/op-bucket", "Bucket delete failed", null);
    HnsRecursiveDeleteException restoredBucketOnly =
        (HnsRecursiveDeleteException) roundTripRawObject(bucketOnlyException);
    assertThat(restoredBucketOnly.getFolderId()).isEqualTo(bucketOnlyId);
    assertThat(restoredBucketOnly.getFolderId().getObjectName()).isEmpty();
    assertThat(restoredBucketOnly.getFolderId().isBucket()).isTrue();
    assertThat(restoredBucketOnly.getOperationName()).hasValue("operations/op-bucket");

    // 3. Folder name containing special URI characters (spaces, ?, #, percent-sequences).
    GcsItemId specialCharsId =
        GcsItemId.builder().setBucketName("b").setObjectName("a b?c#d%20e/").build();
    HnsRecursiveDeleteException specialCharsException =
        new HnsRecursiveDeleteException(
            specialCharsId, "operations/op-special", "Special folder delete failed", null);
    HnsRecursiveDeleteException restoredSpecialChars =
        (HnsRecursiveDeleteException) roundTripRawObject(specialCharsException);
    assertThat(restoredSpecialChars.getFolderId()).isEqualTo(specialCharsId);
    assertThat(restoredSpecialChars.getFolderId().getObjectName()).hasValue("a b?c#d%20e/");
    assertThat(restoredSpecialChars.getOperationName()).hasValue("operations/op-special");
  }

  @Test
  void readObject_whenInvokedDirectlyOrViaForgedStream_throwsInvalidObjectException()
      throws Exception {
    HnsRecursiveDeleteException valid =
        new HnsRecursiveDeleteException(FOLDER_ID, "operations/op-1", "Delete failed", null);

    // 1. Direct reflective invocation of HnsRecursiveDeleteException.readObject.
    Method readObject =
        HnsRecursiveDeleteException.class.getDeclaredMethod("readObject", ObjectInputStream.class);
    readObject.setAccessible(true);
    InvocationTargetException reflectiveThrown =
        assertThrows(
            InvocationTargetException.class, () -> readObject.invoke(valid, (Object) null));
    assertThat(reflectiveThrown.getCause()).isInstanceOf(InvalidObjectException.class);
    assertThat(reflectiveThrown.getCause()).hasMessageThat().isEqualTo("Use SerializationProxy");

    // 2. Forged ObjectInputStream bypassing SerializationProxy triggers readObject rejection.
    byte[] forgedBytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new ForgedStreamStandIn("forged"));
      forgedBytes = baos.toByteArray();
    }
    try (ObjectInputStream forgedOis =
        new ObjectInputStream(new ByteArrayInputStream(forgedBytes)) {
          @Override
          protected ObjectStreamClass readClassDescriptor()
              throws IOException, ClassNotFoundException {
            ObjectStreamClass desc = super.readClassDescriptor();
            if (desc.getName().equals(ForgedStreamStandIn.class.getName())) {
              return ObjectStreamClass.lookup(HnsRecursiveDeleteException.class);
            }
            return desc;
          }
        }) {
      InvalidObjectException streamThrown =
          assertThrows(InvalidObjectException.class, forgedOis::readObject);
      assertThat(streamThrown).hasMessageThat().isEqualTo("Use SerializationProxy");
    }
  }

  private static final class ForgedStreamStandIn extends IOException {
    private static final long serialVersionUID = 1L;

    private ForgedStreamStandIn(String message) {
      super(message);
    }
  }

  private static void setPrivateField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object roundTripRawObject(Object obj) throws Exception {
    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
      bytes = baos.toByteArray();
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return ois.readObject();
    }
  }
}
