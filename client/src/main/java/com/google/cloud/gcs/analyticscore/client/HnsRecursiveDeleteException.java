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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Exception thrown when a recursive folder deletion on a Hierarchical Namespace (HNS) bucket fails
 * or cannot confirm full deletion.
 *
 * <p>Because recursive folder deletion is not atomic, sub-items under {@link #getFolderId()} may
 * already have been deleted before the failure occurred. Callers can inspect {@link #getFolderId()}
 * and {@link #getOperationName()} for diagnostics or recovery.
 */
public final class HnsRecursiveDeleteException extends IOException {

  private static final long serialVersionUID = 1L;

  private final transient GcsItemId folderId;
  @Nullable private final String operationName;

  /**
   * Creates a new {@link HnsRecursiveDeleteException}.
   *
   * @param folderId identifier of the folder targeted by the recursive delete (non-null)
   * @param operationName optional server-assigned long-running operation name, or {@code null} if
   *     unavailable
   * @param message detail message describing the failure
   * @param cause underlying failure cause, or {@code null}
   */
  public HnsRecursiveDeleteException(
      GcsItemId folderId,
      @Nullable String operationName,
      String message,
      @Nullable Throwable cause) {
    super(message, cause);
    this.folderId = checkNotNull(folderId, "folderId must not be null");
    this.operationName = operationName;
  }

  /**
   * Creates a new {@link HnsRecursiveDeleteException} without a server operation name.
   *
   * @param folderId identifier of the folder targeted by the recursive delete (non-null)
   * @param message detail message describing the failure
   * @param cause underlying failure cause, or {@code null}
   */
  public HnsRecursiveDeleteException(
      GcsItemId folderId, String message, @Nullable Throwable cause) {
    this(folderId, null, message, cause);
  }

  /** Returns the identifier of the folder targeted by the recursive delete. */
  public GcsItemId getFolderId() {
    return folderId;
  }

  /** Returns the server-assigned long-running operation name, if one was started. */
  public Optional<String> getOperationName() {
    return Optional.ofNullable(operationName);
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("Use SerializationProxy");
  }

  private static final class SerializationProxy implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String bucketName;
    @Nullable private final String objectName;
    @Nullable private final Long contentGeneration;
    @Nullable private final String operationName;
    @Nullable private final String message;
    @Nullable private final Throwable cause;
    private final StackTraceElement[] stackTrace;
    private final Throwable[] suppressed;

    private SerializationProxy(HnsRecursiveDeleteException exception) {
      GcsItemId id = exception.getFolderId();
      this.bucketName = id.getBucketName();
      this.objectName = id.getObjectName().orElse(null);
      this.contentGeneration = id.getContentGeneration().orElse(null);
      this.operationName = exception.operationName;
      this.message = exception.getMessage();
      this.cause = exception.getCause();
      this.stackTrace = exception.getStackTrace();
      this.suppressed = exception.getSuppressed();
    }

    private Object readResolve() throws InvalidObjectException {
      if (bucketName == null) {
        throw new InvalidObjectException("bucketName must not be null");
      }
      GcsItemId.Builder idBuilder = GcsItemId.builder().setBucketName(bucketName);
      if (objectName != null) {
        idBuilder.setObjectName(objectName);
      }
      if (contentGeneration != null) {
        idBuilder.setContentGeneration(contentGeneration);
      }
      HnsRecursiveDeleteException restored =
          new HnsRecursiveDeleteException(idBuilder.build(), operationName, message, cause);
      if (stackTrace != null) {
        for (StackTraceElement element : stackTrace) {
          if (element == null) {
            throw new InvalidObjectException("stackTrace element must not be null");
          }
        }
        restored.setStackTrace(stackTrace);
      }
      if (suppressed != null) {
        for (Throwable s : suppressed) {
          if (s == null) {
            throw new InvalidObjectException("suppressed exception must not be null");
          }
          restored.addSuppressed(s);
        }
      }
      return restored;
    }
  }
}
