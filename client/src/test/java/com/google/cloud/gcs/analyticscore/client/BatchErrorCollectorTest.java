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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.storage.StorageException;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BatchErrorCollectorTest {

  private static final String OPERATION_SUMMARY = "Failed batch delete";

  @Test
  void initialStateAndOnSuccess_hasNoErrorsAndThrowIfFailedIsNoOp() throws IOException {
    BatchErrorCollector collector = BatchErrorCollector.create();

    collector.onSuccess(null);
    collector.onSuccess("completed");

    assertThat(collector.hasErrors()).isFalse();
    assertThat(collector.getFailureCount()).isEqualTo(0);
    collector.throwIfFailed(OPERATION_SUMMARY);
  }

  @Test
  void concurrentStress_25000FailuresAcross50Threads_capsRetainedAndCountsAll() throws Exception {
    int numThreads = 50;
    int failuresPerThread = 500;
    int expectedTotal = numThreads * failuresPerThread;
    BatchErrorCollector collector = BatchErrorCollector.create();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    List<Future<?>> futures = new ArrayList<>(numThreads);

    try {
      for (int t = 0; t < numThreads; t++) {
        int threadId = t;
        futures.add(
            executor.submit(
                () -> {
                  try {
                    startBarrier.await(30, TimeUnit.SECONDS);
                    for (int i = 0; i < failuresPerThread; i++) {
                      collector.onFailure(new IOException("failure-" + threadId + "-" + i));
                    }
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  } finally {
                    doneLatch.countDown();
                  }
                }));
      }

      assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }

      assertThat(collector.hasErrors()).isTrue();
      assertThat(collector.getFailureCount()).isEqualTo(expectedTotal);

      IOException primary =
          assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
      Throwable[] suppressed = primary.getSuppressed();
      assertThat(suppressed.length).isAtMost(101);
      assertThat(suppressed.length).isEqualTo(BatchErrorCollector.MAX_RETAINED_FAILURES);
      assertThat(suppressed[suppressed.length - 1])
          .hasMessageThat()
          .isEqualTo("... and 24900 more suppressed failures");
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void throwIfFailed_exactlyMaxRetainedFailures_doesNotAppendSyntheticOmittedSummary() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES; i++) {
      collector.onFailure(new IOException("error-" + i));
    }

    IOException primary =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(collector.getFailureCount()).isEqualTo(BatchErrorCollector.MAX_RETAINED_FAILURES);
    assertThat(primary.getSuppressed()).hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES - 1);
    for (Throwable suppressed : primary.getSuppressed()) {
      assertThat(suppressed.getMessage()).doesNotContain("more suppressed failures");
    }
  }

  @Test
  void throwIfFailed_oneAboveMaxRetainedFailures_appendsOneOmittedSummary() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES + 1; i++) {
      collector.onFailure(new IOException("error-" + i));
    }

    IOException primary =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    Throwable[] suppressed = primary.getSuppressed();
    assertThat(suppressed).hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES);
    assertThat(suppressed[suppressed.length - 1])
        .hasMessageThat()
        .isEqualTo("... and 1 more suppressed failures");
  }

  @Test
  void onFailure_beyondMaxRetainedFailures_evictsLowerPriorityForInterruptAndRestoresInterrupt() {
    Thread.interrupted();
    try {
      BatchErrorCollector collector = BatchErrorCollector.create();
      for (int i = 0; i < 150; i++) {
        collector.onFailure(new CancellationException("cancelled-" + i));
      }
      InterruptedException lateInterrupt = new InterruptedException("worker interrupted at #151");
      collector.onFailure(lateInterrupt);

      assertThat(collector.getFailureCount()).isEqualTo(151);
      InterruptedIOException thrown =
          assertThrows(
              InterruptedIOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
      assertThat(thrown).hasCauseThat().isSameInstanceAs(lateInterrupt);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(thrown.getSuppressed()).hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES);
      assertThat(thrown.getSuppressed()[thrown.getSuppressed().length - 1])
          .hasMessageThat()
          .isEqualTo("... and 51 more suppressed failures");
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void onFailure_beyondMaxRetainedFailures_evictsLowerPriorityForAccessDeniedInArrivalOrder() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    for (int i = 0; i < 60; i++) {
      collector.onFailure(new FileNotFoundException("gs://bucket/missing-" + i));
    }
    for (int i = 0; i < 60; i++) {
      collector.onFailure(new IOException("transient-" + i));
    }
    AccessDeniedException firstDenied = new AccessDeniedException("gs://bucket/secret-1");
    AccessDeniedException secondDenied = new AccessDeniedException("gs://bucket/secret-2");
    collector.onFailure(firstDenied);
    collector.onFailure(secondDenied);

    assertThat(collector.getFailureCount()).isEqualTo(122);
    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).isSameInstanceAs(firstDenied);
    assertThat(Arrays.asList(thrown.getSuppressed())).contains(secondDenied);
    assertThat(thrown.getSuppressed()).hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES);
  }

  @Test
  void onFailure_beyondMaxRetainedFailures_retainsAndRethrowsVirtualMachineError() {
    BatchErrorCollector directCollector = BatchErrorCollector.create();
    for (int i = 0; i < 150; i++) {
      directCollector.onFailure(new IOException("transient-" + i));
    }
    OutOfMemoryError directOom = new OutOfMemoryError("late OOM");
    directCollector.onFailure(directOom);

    assertThat(directCollector.getFailureCount()).isEqualTo(151);
    OutOfMemoryError thrownDirect =
        assertThrows(
            OutOfMemoryError.class, () -> directCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownDirect).isSameInstanceAs(directOom);

    BatchErrorCollector wrappedCollector = BatchErrorCollector.create();
    for (int i = 0; i < 150; i++) {
      wrappedCollector.onFailure(new FileNotFoundException("missing-" + i));
    }
    OutOfMemoryError wrappedOom = new OutOfMemoryError("wrapped late OOM");
    wrappedCollector.onFailure(new ExecutionError(wrappedOom));

    OutOfMemoryError thrownWrapped =
        assertThrows(
            OutOfMemoryError.class, () -> wrappedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownWrapped).isSameInstanceAs(wrappedOom);
  }

  @Test
  void throwIfFailed_primaryAlreadyAtMaxRetainedSuppressed_stillAppendsOmittedSummary() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    AccessDeniedException primary = new AccessDeniedException("gs://bucket/denied");
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES; i++) {
      primary.addSuppressed(new IOException("inner-" + i));
    }

    collector.onFailure(primary);
    collector.onFailure(new IOException("outer-1"));
    collector.onFailure(new IOException("outer-2"));
    collector.onFailure(new IOException("outer-3"));

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).isSameInstanceAs(primary);
    Throwable[] suppressed = thrown.getSuppressed();
    assertThat(suppressed).hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES + 1);
    assertThat(suppressed[suppressed.length - 1])
        .hasMessageThat()
        .isEqualTo("... and 3 more suppressed failures");
  }

  @Test
  void throwIfFailed_interruptedBeatsAllOtherPrioritiesAndRestoresCallerInterrupt() {
    Thread.interrupted();
    try {
      BatchErrorCollector collector = BatchErrorCollector.create();
      IOException genericIoe = new IOException("generic");
      FileNotFoundException notFound = new FileNotFoundException("gs://bucket/missing");
      FileAlreadyExistsException alreadyExists =
          new FileAlreadyExistsException("gs://bucket/existing");
      DirectoryNotEmptyException dirNotEmpty = new DirectoryNotEmptyException("gs://bucket/dir");
      AccessDeniedException accessDenied = new AccessDeniedException("gs://bucket/secret");
      InterruptedIOException interrupted = new InterruptedIOException("cancelled");

      collector.onFailure(genericIoe);
      collector.onFailure(notFound);
      collector.onFailure(alreadyExists);
      collector.onFailure(dirNotEmpty);
      collector.onFailure(accessDenied);
      collector.onFailure(interrupted);

      IOException thrown =
          assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

      assertThat(thrown).isSameInstanceAs(interrupted);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(Arrays.asList(thrown.getSuppressed()))
          .containsExactly(genericIoe, notFound, alreadyExists, dirNotEmpty, accessDenied)
          .inOrder();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throwIfFailed_closedByInterruptException_hasInterruptPriorityAndRestoresCallerInterrupt() {
    Thread.interrupted();
    try {
      BatchErrorCollector directCollector = BatchErrorCollector.create();
      AccessDeniedException accessDenied = new AccessDeniedException("gs://bucket/secret");
      ClosedByInterruptException closedByInterrupt = new ClosedByInterruptException();

      directCollector.onFailure(accessDenied);
      directCollector.onFailure(closedByInterrupt);

      IOException thrownDirect =
          assertThrows(IOException.class, () -> directCollector.throwIfFailed(OPERATION_SUMMARY));
      assertThat(thrownDirect).isSameInstanceAs(closedByInterrupt);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(Arrays.asList(thrownDirect.getSuppressed())).containsExactly(accessDenied);

      Thread.interrupted();
      BatchErrorCollector wrappedCollector = BatchErrorCollector.create();
      RuntimeException wrapped =
          new RuntimeException("channel failed", new ClosedByInterruptException());
      wrappedCollector.onFailure(accessDenied);
      wrappedCollector.onFailure(wrapped);

      InterruptedIOException thrownWrapped =
          assertThrows(
              InterruptedIOException.class,
              () -> wrappedCollector.throwIfFailed(OPERATION_SUMMARY));
      assertThat(thrownWrapped).hasCauseThat().isSameInstanceAs(wrapped);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throwIfFailed_accessDeniedBeatsDirNotEmptyAlreadyExistsNotFoundAndGeneric() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    collector.onFailure(new IOException("generic"));
    collector.onFailure(new FileNotFoundException("missing"));
    collector.onFailure(new FileAlreadyExistsException("exists"));
    collector.onFailure(new DirectoryNotEmptyException("non-empty"));
    AccessDeniedException accessDenied = new AccessDeniedException("denied");
    collector.onFailure(accessDenied);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(thrown).isSameInstanceAs(accessDenied);
  }

  @Test
  void throwIfFailed_directoryNotEmptyBeatsAlreadyExistsNotFoundAndGeneric() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    collector.onFailure(new IOException("generic"));
    collector.onFailure(new FileNotFoundException("missing"));
    collector.onFailure(new FileAlreadyExistsException("exists"));
    DirectoryNotEmptyException dirNotEmpty = new DirectoryNotEmptyException("dir");
    collector.onFailure(dirNotEmpty);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(thrown).isSameInstanceAs(dirNotEmpty);
  }

  @Test
  void throwIfFailed_fileAlreadyExistsBeatsNotFoundAndGeneric() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    collector.onFailure(new IOException("generic"));
    collector.onFailure(new FileNotFoundException("missing"));
    FileAlreadyExistsException alreadyExists = new FileAlreadyExistsException("exists");
    collector.onFailure(alreadyExists);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(thrown).isSameInstanceAs(alreadyExists);
  }

  @Test
  void throwIfFailed_fileNotFoundBeatsGenericIOException() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    collector.onFailure(new IOException("generic"));
    FileNotFoundException notFound = new FileNotFoundException("missing");
    collector.onFailure(notFound);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(thrown).isSameInstanceAs(notFound);
  }

  @Test
  void throwIfFailed_noSuchFileExceptionWhenNotIgnored_preservesAndRanksAtNotFoundPriority() {
    BatchErrorCollector directBeatsGeneric = BatchErrorCollector.create();
    IOException generic = new IOException("generic");
    NoSuchFileException noSuchFile = new NoSuchFileException("gs://bucket/missing");
    directBeatsGeneric.onFailure(generic);
    directBeatsGeneric.onFailure(noSuchFile);

    IOException thrownDirect =
        assertThrows(IOException.class, () -> directBeatsGeneric.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownDirect).isSameInstanceAs(noSuchFile);
    assertThat(Arrays.asList(thrownDirect.getSuppressed())).containsExactly(generic);

    BatchErrorCollector losesToAlreadyExists = BatchErrorCollector.create();
    FileAlreadyExistsException alreadyExists =
        new FileAlreadyExistsException("gs://bucket/existing");
    losesToAlreadyExists.onFailure(noSuchFile);
    losesToAlreadyExists.onFailure(alreadyExists);

    IOException thrownConflict =
        assertThrows(
            IOException.class, () -> losesToAlreadyExists.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownConflict).isSameInstanceAs(alreadyExists);

    BatchErrorCollector wrappedCollector = BatchErrorCollector.create();
    RuntimeException wrappedNoSuchFile =
        new RuntimeException("outer", new NoSuchFileException("gs://bucket/nested-missing"));
    wrappedCollector.onFailure(new IOException("generic"));
    wrappedCollector.onFailure(wrappedNoSuchFile);

    FileNotFoundException converted =
        assertThrows(
            FileNotFoundException.class, () -> wrappedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(converted).hasCauseThat().isSameInstanceAs(wrappedNoSuchFile);
    assertThat(converted)
        .hasMessageThat()
        .isEqualTo(OPERATION_SUMMARY + ": gs://bucket/nested-missing");
  }

  @Test
  void throwIfFailed_socketTimeoutException_doesNotInterruptCallerOrBeatAccessDenied() {
    Thread.interrupted();
    try {
      BatchErrorCollector collector = BatchErrorCollector.create();
      SocketTimeoutException socketTimeout = new SocketTimeoutException("Read timed out");
      AccessDeniedException accessDenied = new AccessDeniedException("gs://bucket/forbidden");

      collector.onFailure(socketTimeout);
      collector.onFailure(accessDenied);

      IOException thrown =
          assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

      assertThat(thrown).isSameInstanceAs(accessDenied);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
      assertThat(Arrays.asList(thrown.getSuppressed())).containsExactly(socketTimeout);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throwIfFailed_samePriorityTieBreak_retainsFirstRecordedAsPrimary() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    AccessDeniedException first = new AccessDeniedException("gs://bucket/first");
    AccessDeniedException second = new AccessDeniedException("gs://bucket/second");

    collector.onFailure(first);
    collector.onFailure(second);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(thrown).isSameInstanceAs(first);
    assertThat(Arrays.asList(thrown.getSuppressed())).containsExactly(second);
  }

  @Test
  void ignoringNotFound_ignoresAllNotFoundVariantsAndRetainsForbidden() {
    BatchErrorCollector collector = BatchErrorCollector.ignoringNotFound();
    ApiException gaxNotFoundSubclass =
        ApiExceptionFactory.createException(
            "folder not found", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    ApiException rawApiNotFound =
        new ApiException("object not found", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    StatusRuntimeException grpcRuntimeNotFound = Status.NOT_FOUND.asRuntimeException();
    StatusException grpcCheckedNotFound = Status.NOT_FOUND.asException();

    collector.onFailure(new FileNotFoundException("gs://bucket/a"));
    collector.onFailure(new NoSuchFileException("gs://bucket/b"));
    collector.onFailure(new StorageException(404, "Not Found"));
    collector.onFailure(new StorageException(0, "gRPC wrapper", gaxNotFoundSubclass));
    collector.onFailure(gaxNotFoundSubclass);
    collector.onFailure(rawApiNotFound);
    collector.onFailure(grpcRuntimeNotFound);
    collector.onFailure(grpcCheckedNotFound);
    collector.onFailure(new RuntimeException("wrapped", new StorageException(404, "Not Found")));

    assertThat(collector.hasErrors()).isFalse();
    assertThat(collector.getFailureCount()).isEqualTo(0);

    StorageException forbidden = new StorageException(403, "Forbidden");
    collector.onFailure(forbidden);

    assertThat(collector.hasErrors()).isTrue();
    assertThat(collector.getFailureCount()).isEqualTo(1);
    AccessDeniedException thrown =
        assertThrows(AccessDeniedException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).hasCauseThat().isSameInstanceAs(forbidden);
    assertThat(thrown.getFile()).isNull();
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  @Test
  void ignoringNotFound_compositeWithSuppressed_ignoresPure404AndExtractsNon404Failures() {
    FileNotFoundException pure404Composite = new FileNotFoundException("gs://bucket/missing");
    pure404Composite.addSuppressed(new StorageException(404, "Not Found"));
    pure404Composite.addSuppressed(Status.NOT_FOUND.asRuntimeException());
    assertThat(BatchErrorCollector.isNotFound(pure404Composite)).isTrue();

    FileNotFoundException cyclic404Composite = new FileNotFoundException("gs://bucket/cycle");
    cyclic404Composite.addSuppressed(new RuntimeException("back-edge", cyclic404Composite));
    assertThat(BatchErrorCollector.isNotFound(cyclic404Composite)).isTrue();

    BatchErrorCollector pureCollector = BatchErrorCollector.ignoringNotFound();
    pureCollector.onFailure(pure404Composite);
    pureCollector.onFailure(cyclic404Composite);
    assertThat(pureCollector.hasErrors()).isFalse();
    assertThat(pureCollector.getFailureCount()).isEqualTo(0);

    FileNotFoundException fallback404With500 = new FileNotFoundException("gs://bucket/missing");
    IOException session500 = new IOException("HTTP 500 session failure");
    fallback404With500.addSuppressed(session500);
    assertThat(BatchErrorCollector.isNotFound(fallback404With500)).isFalse();

    BatchErrorCollector singleSuppressedCollector = BatchErrorCollector.ignoringNotFound();
    singleSuppressedCollector.onFailure(fallback404With500);
    assertThat(singleSuppressedCollector.hasErrors()).isTrue();
    assertThat(singleSuppressedCollector.getFailureCount()).isEqualTo(1);
    IOException thrown500 =
        assertThrows(
            IOException.class, () -> singleSuppressedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown500).isSameInstanceAs(session500);

    FileNotFoundException innerComposite = new FileNotFoundException("inner 404");
    AccessDeniedException nestedDenied = new AccessDeniedException("gs://bucket/secret");
    innerComposite.addSuppressed(new StorageException(404, "inner suppressed 404"));
    innerComposite.addSuppressed(nestedDenied);

    FileNotFoundException outerComposite = new FileNotFoundException("outer 404");
    IOException outerGeneric = new IOException("outer 503");
    outerComposite.addSuppressed(innerComposite);
    outerComposite.addSuppressed(outerGeneric);

    BatchErrorCollector nestedCollector = BatchErrorCollector.ignoringNotFound();
    nestedCollector.onFailure(new ExecutionException(outerComposite));
    assertThat(nestedCollector.getFailureCount()).isEqualTo(2);
    IOException thrownNested =
        assertThrows(IOException.class, () -> nestedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownNested).isSameInstanceAs(nestedDenied);
    assertThat(Arrays.asList(thrownNested.getSuppressed())).containsExactly(outerGeneric);

    FileNotFoundException compositeWithOom = new FileNotFoundException("gs://bucket/missing");
    OutOfMemoryError suppressedOom = new OutOfMemoryError("suppressed OOM");
    compositeWithOom.addSuppressed(suppressedOom);
    assertThat(BatchErrorCollector.isNotFound(compositeWithOom)).isFalse();

    BatchErrorCollector oomCollector = BatchErrorCollector.ignoringNotFound();
    oomCollector.onFailure(compositeWithOom);
    OutOfMemoryError thrownOom =
        assertThrows(OutOfMemoryError.class, () -> oomCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownOom).isSameInstanceAs(suppressedOom);
  }

  @Test
  void create_withIgnoreNotFoundFalse_retainsAndTranslatesNotFoundErrors() {
    BatchErrorCollector collector = BatchErrorCollector.create(false);
    StorageException storage404 = new StorageException(404, "Not Found");
    ApiException apiNotFound =
        ApiExceptionFactory.createException(
            "missing", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    StatusRuntimeException grpcNotFound = Status.NOT_FOUND.asRuntimeException();

    collector.onFailure(storage404);
    collector.onFailure(apiNotFound);
    collector.onFailure(grpcNotFound);

    assertThat(collector.getFailureCount()).isEqualTo(3);
    IOException thrown =
        assertThrows(FileNotFoundException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).hasCauseThat().isSameInstanceAs(storage404);
    assertThat(thrown.getSuppressed()).hasLength(2);
    assertThat(thrown.getSuppressed()[0]).isInstanceOf(FileNotFoundException.class);
    assertThat(thrown.getSuppressed()[1]).isInstanceOf(FileNotFoundException.class);
  }

  @Test
  void throwIfFailed_interruptedException_convertsToInterruptedIOExceptionAndRestoresInterrupt() {
    Thread.interrupted();
    try {
      BatchErrorCollector collector = BatchErrorCollector.create();
      InterruptedException ie = new InterruptedException("worker interrupted");
      collector.onFailure(ie);

      InterruptedIOException thrown =
          assertThrows(
              InterruptedIOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

      assertThat(thrown).hasCauseThat().isSameInstanceAs(ie);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throwIfFailed_cancelledAndUnknownGrpcWrappers_unwrapsInterruptVmErrorAndNotFoundCauses() {
    Thread.interrupted();
    try {
      InterruptedException rpcInterrupt = new InterruptedException("grpc call interrupted");
      StatusRuntimeException grpcCancelled =
          Status.CANCELLED.withCause(rpcInterrupt).asRuntimeException();
      StatusException grpcCheckedCancelled = Status.CANCELLED.withCause(rpcInterrupt).asException();
      ApiException gaxCancelled =
          ApiExceptionFactory.createException(
              "RPC cancelled", grpcCancelled, GrpcStatusCode.of(Status.Code.CANCELLED), false);
      StorageException storageCancelled = new StorageException(0, "cancelled", gaxCancelled);

      for (Throwable interruptWrapper :
          Arrays.asList(grpcCancelled, grpcCheckedCancelled, gaxCancelled, storageCancelled)) {
        Thread.interrupted();
        BatchErrorCollector collector = BatchErrorCollector.create();
        collector.onFailure(new AccessDeniedException("gs://bucket/denied"));
        collector.onFailure(interruptWrapper);

        InterruptedIOException thrown =
            assertThrows(
                InterruptedIOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
        assertThat(thrown).hasCauseThat().isSameInstanceAs(interruptWrapper);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
      }

      Thread.interrupted();
      BatchErrorCollector nonInterruptCancelled = BatchErrorCollector.create();
      StatusRuntimeException serverCancelled =
          Status.CANCELLED.withDescription("cancelled by peer").asRuntimeException();
      FileNotFoundException fnfe = new FileNotFoundException("gs://bucket/missing");
      nonInterruptCancelled.onFailure(serverCancelled);
      nonInterruptCancelled.onFailure(fnfe);

      IOException primary =
          assertThrows(
              IOException.class, () -> nonInterruptCancelled.throwIfFailed(OPERATION_SUMMARY));
      assertThat(primary).isSameInstanceAs(fnfe);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      OutOfMemoryError grpcOom = new OutOfMemoryError("grpc stream OOM");
      StatusRuntimeException unknownWrappingOom =
          Status.UNKNOWN.withCause(grpcOom).asRuntimeException();
      ApiException gaxUnknownWrappingOom =
          ApiExceptionFactory.createException(
              "unknown", unknownWrappingOom, GrpcStatusCode.of(Status.Code.UNKNOWN), false);
      BatchErrorCollector oomCollector = BatchErrorCollector.create();
      oomCollector.onFailure(gaxUnknownWrappingOom);
      assertThat(
              assertThrows(
                  OutOfMemoryError.class, () -> oomCollector.throwIfFailed(OPERATION_SUMMARY)))
          .isSameInstanceAs(grpcOom);

      StatusRuntimeException unknownWrappingNotFound =
          Status.UNKNOWN.withCause(new FileNotFoundException("missing")).asRuntimeException();
      ApiException gaxUnknownWrappingNotFound =
          ApiExceptionFactory.createException(
              "unknown", unknownWrappingNotFound, GrpcStatusCode.of(Status.Code.UNKNOWN), false);
      assertThat(BatchErrorCollector.isNotFound(unknownWrappingNotFound)).isTrue();
      assertThat(BatchErrorCollector.isNotFound(gaxUnknownWrappingNotFound)).isTrue();

      BatchErrorCollector notFoundCollector = BatchErrorCollector.create(false);
      notFoundCollector.onFailure(gaxUnknownWrappingNotFound);
      assertThrows(
          FileNotFoundException.class, () -> notFoundCollector.throwIfFailed(OPERATION_SUMMARY));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throwIfFailed_plainIOExceptionWrappingInterruptOrVmError_promotesInterruptAndRethrowsOom() {
    Thread.interrupted();
    try {
      InterruptedException workerInterrupt = new InterruptedException("bidi stream interrupted");
      IOException plainIoeWithInterrupt =
          new IOException("Thread interrupted while reading from stream", workerInterrupt);

      BatchErrorCollector c1 = BatchErrorCollector.create();
      c1.onFailure(new AccessDeniedException("gs://bucket/secret"));
      c1.onFailure(plainIoeWithInterrupt);

      InterruptedIOException thrown1 =
          assertThrows(InterruptedIOException.class, () -> c1.throwIfFailed(OPERATION_SUMMARY));
      assertThat(thrown1).hasCauseThat().isSameInstanceAs(plainIoeWithInterrupt);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      Thread.interrupted();
      StatusRuntimeException grpcCancelled =
          Status.CANCELLED
              .withCause(new InterruptedException("session interrupted"))
              .asRuntimeException();
      ApiException gaxCancelled =
          ApiExceptionFactory.createException(
              "cancelled", grpcCancelled, GrpcStatusCode.of(Status.Code.CANCELLED), false);
      IOException sessionIoe =
          new IOException(
              "Failed to get BlobReadSession",
              new ExecutionException(new StorageException(0, "cancelled", gaxCancelled)));

      BatchErrorCollector c2 = BatchErrorCollector.create();
      c2.onFailure(new AccessDeniedException("gs://bucket/secret"));
      c2.onFailure(sessionIoe);

      InterruptedIOException thrown2 =
          assertThrows(InterruptedIOException.class, () -> c2.throwIfFailed(OPERATION_SUMMARY));
      assertThat(thrown2).hasCauseThat().isSameInstanceAs(sessionIoe);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      Thread.interrupted();
      IOException timeoutWrapper =
          new IOException("Timed out", new SocketTimeoutException("read timed out"));
      IOException deadlineWrapper =
          new IOException(
              "RPC deadline",
              Status.DEADLINE_EXCEEDED
                  .withCause(new InterruptedException("ignored beneath deadline"))
                  .asRuntimeException());
      AccessDeniedException denied = new AccessDeniedException("gs://bucket/denied");

      BatchErrorCollector c3 = BatchErrorCollector.create();
      c3.onFailure(timeoutWrapper);
      c3.onFailure(deadlineWrapper);
      c3.onFailure(denied);

      IOException thrown3 =
          assertThrows(IOException.class, () -> c3.throwIfFailed(OPERATION_SUMMARY));
      assertThat(thrown3).isSameInstanceAs(denied);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      OutOfMemoryError nestedOom = new OutOfMemoryError("nested inside IOException");
      BatchErrorCollector c4 = BatchErrorCollector.create();
      c4.onFailure(new IOException("ioe wrapping oom", nestedOom));
      assertThat(assertThrows(OutOfMemoryError.class, () -> c4.throwIfFailed(OPERATION_SUMMARY)))
          .isSameInstanceAs(nestedOom);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void throwIfFailed_storageExceptions_maps401And403ToAccessDeniedAnd409ByUnderlyingCause() {
    BatchErrorCollector unauthorizedCollector = BatchErrorCollector.create();
    StorageException storage401 = new StorageException(401, "Unauthorized");
    unauthorizedCollector.onFailure(storage401);

    AccessDeniedException thrown401 =
        assertThrows(
            AccessDeniedException.class,
            () -> unauthorizedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown401).hasCauseThat().isSameInstanceAs(storage401);
    assertThat(thrown401.getFile()).isNull();

    BatchErrorCollector httpConflictCollector = BatchErrorCollector.create();
    StorageException httpStorage409 = new StorageException(409, "Conflict");
    httpConflictCollector.onFailure(httpStorage409);

    FileAlreadyExistsException thrownHttp409 =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> httpConflictCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownHttp409).hasCauseThat().isSameInstanceAs(httpStorage409);
    assertThat(thrownHttp409.getFile()).isNull();

    ApiException gaxAlreadyExists =
        ApiExceptionFactory.createException(
            "object already exists",
            Status.ALREADY_EXISTS.asRuntimeException(),
            GrpcStatusCode.of(Status.Code.ALREADY_EXISTS),
            false);
    StorageException grpcAlreadyExists409 =
        new StorageException(409, "Already Exists", gaxAlreadyExists);
    BatchErrorCollector grpcExistsCollector = BatchErrorCollector.create();
    grpcExistsCollector.onFailure(grpcAlreadyExists409);

    FileAlreadyExistsException thrownGrpcExists =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> grpcExistsCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownGrpcExists).hasCauseThat().isSameInstanceAs(grpcAlreadyExists409);
    assertThat(thrownGrpcExists.getFile()).isNull();

    ApiException gaxAborted =
        ApiExceptionFactory.createException(
            "concurrent metadata update",
            Status.ABORTED.asRuntimeException(),
            GrpcStatusCode.of(Status.Code.ABORTED),
            true);
    StorageException grpcAborted409 = new StorageException(409, "Aborted", gaxAborted);
    BatchErrorCollector grpcAbortedCollector = BatchErrorCollector.create();
    grpcAbortedCollector.onFailure(grpcAborted409);

    IOException thrownAborted =
        assertThrows(
            IOException.class, () -> grpcAbortedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownAborted.getClass()).isEqualTo(IOException.class);
    assertThat(thrownAborted).hasCauseThat().isSameInstanceAs(grpcAborted409);

    StorageException rawGrpcAborted409 =
        new StorageException(409, "Aborted", Status.ABORTED.asRuntimeException());
    BatchErrorCollector priorityCheckCollector = BatchErrorCollector.create();
    FileNotFoundException notFound = new FileNotFoundException("gs://bucket/missing");
    priorityCheckCollector.onFailure(grpcAborted409);
    priorityCheckCollector.onFailure(rawGrpcAborted409);
    priorityCheckCollector.onFailure(notFound);

    IOException winner =
        assertThrows(
            IOException.class, () -> priorityCheckCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(winner).isSameInstanceAs(notFound);
  }

  @Test
  void throwIfFailed_fileSystemExceptions_preservesWrappedPathsAndAvoidsDanglingColon() {
    BatchErrorCollector emptyMsg403 = BatchErrorCollector.create();
    emptyMsg403.onFailure(new StorageException(403, ""));
    AccessDeniedException translatedDenied =
        assertThrows(
            AccessDeniedException.class, () -> emptyMsg403.throwIfFailed(OPERATION_SUMMARY));
    assertThat(translatedDenied.getFile()).isNull();
    assertThat(translatedDenied.getOtherFile()).isNull();
    assertThat(translatedDenied.getMessage()).isEqualTo(OPERATION_SUMMARY);

    BatchErrorCollector emptyMsg409 = BatchErrorCollector.create();
    emptyMsg409.onFailure(new StorageException(409, ""));
    FileAlreadyExistsException translatedExists =
        assertThrows(
            FileAlreadyExistsException.class, () -> emptyMsg409.throwIfFailed(OPERATION_SUMMARY));
    assertThat(translatedExists.getFile()).isNull();
    assertThat(translatedExists.getOtherFile()).isNull();
    assertThat(translatedExists.getMessage()).isEqualTo(OPERATION_SUMMARY);

    BatchErrorCollector wrappedAdeCollector = BatchErrorCollector.create();
    AccessDeniedException innerAde =
        new AccessDeniedException("gs://bucket/src", "gs://bucket/dst", "IAM denied");
    RuntimeException outerAde = new RuntimeException("task failed", innerAde);
    wrappedAdeCollector.onFailure(outerAde);
    AccessDeniedException wrappedAde =
        assertThrows(
            AccessDeniedException.class,
            () -> wrappedAdeCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(wrappedAde.getFile()).isEqualTo("gs://bucket/src");
    assertThat(wrappedAde.getOtherFile()).isEqualTo("gs://bucket/dst");
    assertThat(wrappedAde.getReason()).isEqualTo(OPERATION_SUMMARY + ": IAM denied");
    assertThat(wrappedAde).hasCauseThat().isSameInstanceAs(outerAde);

    BatchErrorCollector wrappedFaeeCollector = BatchErrorCollector.create();
    FileAlreadyExistsException innerFaee =
        new FileAlreadyExistsException("gs://bucket/target", "gs://bucket/source", "conflict");
    RuntimeException outerFaee = new RuntimeException("task failed", innerFaee);
    wrappedFaeeCollector.onFailure(outerFaee);
    FileAlreadyExistsException wrappedFaee =
        assertThrows(
            FileAlreadyExistsException.class,
            () -> wrappedFaeeCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(wrappedFaee.getFile()).isEqualTo("gs://bucket/target");
    assertThat(wrappedFaee.getOtherFile()).isEqualTo("gs://bucket/source");
    assertThat(wrappedFaee.getReason()).isEqualTo(OPERATION_SUMMARY + ": conflict");
    assertThat(wrappedFaee).hasCauseThat().isSameInstanceAs(outerFaee);

    BatchErrorCollector wrappedDirCollector = BatchErrorCollector.create();
    DirectoryNotEmptyException innerDnee =
        new DirectoryNotEmptyException("gs://bucket/non-empty-dir");
    RuntimeException outerDnee = new RuntimeException("delete failed", innerDnee);
    wrappedDirCollector.onFailure(outerDnee);
    DirectoryNotEmptyException wrappedDnee =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> wrappedDirCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(wrappedDnee.getFile()).isEqualTo("gs://bucket/non-empty-dir");
    assertThat(wrappedDnee.getMessage()).isEqualTo("gs://bucket/non-empty-dir");
    assertThat(wrappedDnee).hasCauseThat().isSameInstanceAs(outerDnee);
  }

  @Test
  void throwIfFailed_gaxAndGrpcStatusCodes_mapsToSemanticIOExceptionSubtypes() {
    ApiException permDenied =
        ApiExceptionFactory.createException(
            "denied", null, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false);
    ApiException unauth =
        ApiExceptionFactory.createException(
            "unauth", null, GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), false);
    ApiException alreadyExists =
        ApiExceptionFactory.createException(
            "exists", null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
    ApiException failedPrecondition =
        ApiExceptionFactory.createException(
            "precondition", null, GrpcStatusCode.of(Status.Code.FAILED_PRECONDITION), false);
    StatusException grpcDenied = Status.PERMISSION_DENIED.withDescription("denied").asException();
    StatusRuntimeException grpcUnauth =
        Status.UNAUTHENTICATED.withDescription("unauthenticated").asRuntimeException();
    StatusRuntimeException grpcAlreadyExists =
        Status.ALREADY_EXISTS.withDescription("already exists").asRuntimeException();
    StatusRuntimeException grpcInternal =
        Status.INTERNAL.withDescription("internal error").asRuntimeException();

    BatchErrorCollector c1 = BatchErrorCollector.create();
    c1.onFailure(permDenied);
    AccessDeniedException ade1 =
        assertThrows(AccessDeniedException.class, () -> c1.throwIfFailed(OPERATION_SUMMARY));
    assertThat(ade1).hasCauseThat().isSameInstanceAs(permDenied);
    assertThat(ade1.getFile()).isNull();

    BatchErrorCollector c2 = BatchErrorCollector.create();
    c2.onFailure(unauth);
    AccessDeniedException ade2 =
        assertThrows(AccessDeniedException.class, () -> c2.throwIfFailed(OPERATION_SUMMARY));
    assertThat(ade2).hasCauseThat().isSameInstanceAs(unauth);
    assertThat(ade2.getFile()).isNull();

    BatchErrorCollector c3 = BatchErrorCollector.create();
    c3.onFailure(alreadyExists);
    FileAlreadyExistsException faee3 =
        assertThrows(FileAlreadyExistsException.class, () -> c3.throwIfFailed(OPERATION_SUMMARY));
    assertThat(faee3).hasCauseThat().isSameInstanceAs(alreadyExists);
    assertThat(faee3.getFile()).isNull();

    BatchErrorCollector c4 = BatchErrorCollector.create();
    c4.onFailure(failedPrecondition);
    IOException precondIoe =
        assertThrows(IOException.class, () -> c4.throwIfFailed(OPERATION_SUMMARY));
    assertThat(precondIoe.getClass()).isEqualTo(IOException.class);
    assertThat(precondIoe).hasCauseThat().isSameInstanceAs(failedPrecondition);

    BatchErrorCollector c5 = BatchErrorCollector.create();
    c5.onFailure(grpcDenied);
    AccessDeniedException ade5 =
        assertThrows(AccessDeniedException.class, () -> c5.throwIfFailed(OPERATION_SUMMARY));
    assertThat(ade5).hasCauseThat().isSameInstanceAs(grpcDenied);
    assertThat(ade5.getFile()).isNull();

    BatchErrorCollector c6 = BatchErrorCollector.create();
    c6.onFailure(grpcUnauth);
    AccessDeniedException ade6 =
        assertThrows(AccessDeniedException.class, () -> c6.throwIfFailed(OPERATION_SUMMARY));
    assertThat(ade6).hasCauseThat().isSameInstanceAs(grpcUnauth);
    assertThat(ade6.getFile()).isNull();

    BatchErrorCollector c7 = BatchErrorCollector.create();
    c7.onFailure(grpcAlreadyExists);
    FileAlreadyExistsException faee7 =
        assertThrows(FileAlreadyExistsException.class, () -> c7.throwIfFailed(OPERATION_SUMMARY));
    assertThat(faee7).hasCauseThat().isSameInstanceAs(grpcAlreadyExists);
    assertThat(faee7.getFile()).isNull();

    BatchErrorCollector c8 = BatchErrorCollector.create();
    c8.onFailure(grpcInternal);
    IOException internalIoe =
        assertThrows(IOException.class, () -> c8.throwIfFailed(OPERATION_SUMMARY));
    assertThat(internalIoe.getClass()).isEqualTo(IOException.class);
    assertThat(internalIoe).hasCauseThat().isSameInstanceAs(grpcInternal);

    StatusException grpcCheckedUnauth =
        Status.UNAUTHENTICATED.withDescription("checked unauth").asException();
    StatusException grpcCheckedExists =
        Status.ALREADY_EXISTS.withDescription("checked exists").asException();
    StatusException grpcCheckedInternal =
        Status.INTERNAL.withDescription("checked internal").asException();

    BatchErrorCollector c9 = BatchErrorCollector.create();
    c9.onFailure(grpcCheckedUnauth);
    assertThat(assertThrows(AccessDeniedException.class, () -> c9.throwIfFailed(OPERATION_SUMMARY)))
        .hasCauseThat()
        .isSameInstanceAs(grpcCheckedUnauth);

    BatchErrorCollector c10 = BatchErrorCollector.create();
    c10.onFailure(grpcCheckedExists);
    assertThat(
            assertThrows(
                FileAlreadyExistsException.class, () -> c10.throwIfFailed(OPERATION_SUMMARY)))
        .hasCauseThat()
        .isSameInstanceAs(grpcCheckedExists);

    BatchErrorCollector c11 = BatchErrorCollector.create();
    c11.onFailure(grpcCheckedInternal);
    IOException checkedInternalIoe =
        assertThrows(IOException.class, () -> c11.throwIfFailed(OPERATION_SUMMARY));
    assertThat(checkedInternalIoe.getClass()).isEqualTo(IOException.class);
    assertThat(checkedInternalIoe).hasCauseThat().isSameInstanceAs(grpcCheckedInternal);
  }

  @Test
  void throwIfFailed_transparentWrappers_unwrapsCompletionUncheckedExecutionAndExecutionError() {
    AccessDeniedException denied = new AccessDeniedException("gs://bucket/denied");
    BatchErrorCollector completionCollector = BatchErrorCollector.create();
    completionCollector.onFailure(new CompletionException(denied));
    assertThat(
            assertThrows(
                IOException.class, () -> completionCollector.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(denied);

    DirectoryNotEmptyException dirNotEmpty = new DirectoryNotEmptyException("gs://bucket/dir");
    BatchErrorCollector uncheckedExecCollector = BatchErrorCollector.create();
    uncheckedExecCollector.onFailure(new UncheckedExecutionException(dirNotEmpty));
    assertThat(
            assertThrows(
                IOException.class, () -> uncheckedExecCollector.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(dirNotEmpty);

    OutOfMemoryError oom = new OutOfMemoryError("future OOM");
    BatchErrorCollector execErrorVmCollector = BatchErrorCollector.create();
    execErrorVmCollector.onFailure(new ExecutionError(oom));
    assertThat(
            assertThrows(
                OutOfMemoryError.class,
                () -> execErrorVmCollector.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(oom);

    AssertionError assertionError = new AssertionError("non-VM error");
    ExecutionError wrappedAssertion = new ExecutionError(assertionError);
    BatchErrorCollector execErrorNonVmCollector = BatchErrorCollector.create();
    execErrorNonVmCollector.onFailure(wrappedAssertion);
    IOException wrappedNonVm =
        assertThrows(
            IOException.class, () -> execErrorNonVmCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(wrappedNonVm).hasCauseThat().isSameInstanceAs(wrappedAssertion);
  }

  @Test
  void throwIfFailed_runtimeExceptionAndNonVmError_wrapsInIOExceptionWithOperationSummary() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    IllegalStateException runtimeFailure = new IllegalStateException("unexpected state");
    AssertionError assertionFailure = new AssertionError("assertion failed");

    collector.onFailure(runtimeFailure);
    collector.onFailure(assertionFailure);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).hasMessageThat().isEqualTo(OPERATION_SUMMARY);
    assertThat(thrown).hasCauseThat().isSameInstanceAs(runtimeFailure);
    assertThat(thrown.getSuppressed()).hasLength(1);
    assertThat(thrown.getSuppressed()[0]).hasCauseThat().isSameInstanceAs(assertionFailure);
  }

  @Test
  void throwIfFailed_withVirtualMachineError_rethrowsDirectlyWithoutPrematureInterruptMutation() {
    Thread.interrupted();
    try {
      BatchErrorCollector collector = BatchErrorCollector.create();
      OutOfMemoryError oom = new OutOfMemoryError("simulated OOM");
      collector.onFailure(new InterruptedException("earlier interrupt"));
      collector.onFailure(new IOException("earlier io error"));
      collector.onFailure(oom);

      OutOfMemoryError thrown =
          assertThrows(OutOfMemoryError.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

      assertThat(thrown).isSameInstanceAs(oom);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void isNotFound_withSelfReferencingAndMutualCauseCycles_terminatesAndClassifiesAccurately() {
    Throwable selfReferencing =
        new RuntimeException("self-referencing") {
          @Override
          public synchronized Throwable getCause() {
            return this;
          }
        };

    Exception mutualB = new Exception("mutual-B");
    Exception mutualA = new Exception("mutual-A", mutualB);
    mutualB.initCause(mutualA);

    Exception notFoundBeforeCycle = new Exception("outer", new FileNotFoundException("missing"));
    notFoundBeforeCycle.getCause().initCause(mutualA);

    AccessDeniedException deniedWithNotFoundCause = new AccessDeniedException("gs://bucket/secret");
    deniedWithNotFoundCause.initCause(new FileNotFoundException("inner"));

    assertThat(BatchErrorCollector.isNotFound(null)).isFalse();
    assertThat(BatchErrorCollector.isNotFound(selfReferencing)).isFalse();
    assertThat(BatchErrorCollector.isNotFound(mutualA)).isFalse();
    assertThat(BatchErrorCollector.isNotFound(notFoundBeforeCycle)).isTrue();
    assertThat(BatchErrorCollector.isNotFound(deniedWithNotFoundCause)).isFalse();

    BatchErrorCollector collector = BatchErrorCollector.ignoringNotFound();
    collector.onFailure(selfReferencing);
    collector.onFailure(mutualA);
    assertThat(collector.getFailureCount()).isEqualTo(2);
    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).hasCauseThat().isSameInstanceAs(selfReferencing);
  }

  @Test
  void onFailure_duplicateThrowableInstances_neverSelfSuppressesOrDuplicatesReferences() {
    BatchErrorCollector samePrimaryCollector = BatchErrorCollector.create();
    IOException sharedIoe = new IOException("shared failure");
    samePrimaryCollector.onFailure(sharedIoe);
    samePrimaryCollector.onFailure(sharedIoe);

    assertThat(samePrimaryCollector.getFailureCount()).isEqualTo(2);
    IOException thrownPrimary =
        assertThrows(
            IOException.class, () -> samePrimaryCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownPrimary).isSameInstanceAs(sharedIoe);
    assertThat(thrownPrimary.getSuppressed()).isEmpty();

    BatchErrorCollector mixedCollector = BatchErrorCollector.create();
    AccessDeniedException primaryDenied = new AccessDeniedException("gs://bucket/denied");
    IOException repeatedSecondary = new IOException("repeated secondary");
    mixedCollector.onFailure(repeatedSecondary);
    mixedCollector.onFailure(repeatedSecondary);
    mixedCollector.onFailure(new UncheckedIOException(repeatedSecondary));
    mixedCollector.onFailure(new ExecutionException(repeatedSecondary));
    mixedCollector.onFailure(primaryDenied);

    assertThat(mixedCollector.getFailureCount()).isEqualTo(5);
    IOException firstThrow =
        assertThrows(IOException.class, () -> mixedCollector.throwIfFailed(OPERATION_SUMMARY));
    IOException secondThrow =
        assertThrows(IOException.class, () -> mixedCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(firstThrow).isSameInstanceAs(primaryDenied);
    assertThat(secondThrow).isSameInstanceAs(primaryDenied);
    assertThat(Arrays.asList(firstThrow.getSuppressed())).containsExactly(repeatedSecondary);
  }

  @Test
  void
      onFailure_sharedNonIOExceptionAcrossTransparentWrappersAndCacheMiss_deduplicatesSuppressed() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    AccessDeniedException primaryDenied = new AccessDeniedException("gs://bucket/denied");
    StorageException sharedBackend500 = new StorageException(500, "shared backend failure");

    collector.onFailure(sharedBackend500);
    collector.onFailure(new ExecutionException(sharedBackend500));
    collector.onFailure(new CompletionException(sharedBackend500));
    collector.onFailure(new UncheckedExecutionException(sharedBackend500));
    collector.onFailure(primaryDenied);

    assertThat(collector.getFailureCount()).isEqualTo(5);
    IOException firstThrow =
        assertThrows(IOException.class, () -> collector.throwIfFailed("summary-1"));
    assertThat(firstThrow).isSameInstanceAs(primaryDenied);
    assertThat(firstThrow.getSuppressed()).hasLength(1);
    assertThat(firstThrow.getSuppressed()[0]).hasCauseThat().isSameInstanceAs(sharedBackend500);

    IOException cacheMissThrow =
        assertThrows(IOException.class, () -> collector.throwIfFailed("summary-2"));
    assertThat(cacheMissThrow).isSameInstanceAs(primaryDenied);
    assertThat(cacheMissThrow.getSuppressed()).hasLength(1);
  }

  @Test
  void throwIfFailed_secondaryWrapsPrimaryAsCause_skipsCycleInducingSuppression() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    AccessDeniedException primary = new AccessDeniedException("gs://bucket/secret");
    IOException wrapperReferencingPrimary = new IOException("outer wrapper", primary);

    collector.onFailure(wrapperReferencingPrimary);
    collector.onFailure(primary);

    IOException thrown =
        assertThrows(IOException.class, () -> collector.throwIfFailed(OPERATION_SUMMARY));

    assertThat(thrown).isSameInstanceAs(primary);
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  @Test
  void futuresAddCallback_withVoidAndTypedListenableFutures_wiresVarianceCleanly()
      throws IOException {
    BatchErrorCollector okCollector = BatchErrorCollector.ignoringNotFound();
    ListenableFuture<Void> voidOk = Futures.immediateVoidFuture();
    ListenableFuture<String> stringOk = Futures.immediateFuture("done");
    ListenableFuture<Void> ignored404 =
        Futures.immediateFailedFuture(new StorageException(404, "Not Found"));

    Futures.addCallback(voidOk, okCollector, MoreExecutors.directExecutor());
    Futures.addCallback(stringOk, okCollector, MoreExecutors.directExecutor());
    Futures.addCallback(ignored404, okCollector, MoreExecutors.directExecutor());

    assertThat(okCollector.hasErrors()).isFalse();
    assertThat(okCollector.getFailureCount()).isEqualTo(0);
    okCollector.throwIfFailed(OPERATION_SUMMARY);

    BatchErrorCollector failingCollector = BatchErrorCollector.ignoringNotFound();
    SettableFuture<Void> asyncVoidFailure = SettableFuture.create();
    SettableFuture<String> asyncStringFailure = SettableFuture.create();
    Futures.addCallback(asyncVoidFailure, failingCollector, MoreExecutors.directExecutor());
    Futures.addCallback(asyncStringFailure, failingCollector, MoreExecutors.directExecutor());

    IOException genericIoe = new IOException("read failed");
    AccessDeniedException denied = new AccessDeniedException("gs://bucket/forbidden");
    asyncStringFailure.setException(genericIoe);
    asyncVoidFailure.setException(denied);

    assertThat(failingCollector.hasErrors()).isTrue();
    assertThat(failingCollector.getFailureCount()).isEqualTo(2);
    IOException thrown =
        assertThrows(IOException.class, () -> failingCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrown).isSameInstanceAs(denied);
    assertThat(Arrays.asList(thrown.getSuppressed())).containsExactly(genericIoe);
  }

  @Test
  void ignoringNotFound_compositeWithCausalTailNon404AndSuppressed_retainsNon404AndVmErrorCauses() {
    AccessDeniedException backendDenied = new AccessDeniedException("gs://bucket/denied");
    FileNotFoundException fnfeWithSharedCauseAndSuppressed =
        new FileNotFoundException("gs://bucket/missing");
    fnfeWithSharedCauseAndSuppressed.initCause(backendDenied);
    fnfeWithSharedCauseAndSuppressed.addSuppressed(backendDenied);
    assertThat(BatchErrorCollector.isNotFound(fnfeWithSharedCauseAndSuppressed)).isFalse();

    BatchErrorCollector c1 = BatchErrorCollector.ignoringNotFound();
    c1.onFailure(fnfeWithSharedCauseAndSuppressed);
    assertThat(c1.hasErrors()).isTrue();
    assertThat(c1.getFailureCount()).isEqualTo(1);
    assertThat(assertThrows(IOException.class, () -> c1.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(backendDenied);

    StorageException backend500 = new StorageException(500, "Internal Server Error");
    FileNotFoundException fnfeWithWrappedTailSuppressed =
        new FileNotFoundException("gs://bucket/missing-2");
    fnfeWithWrappedTailSuppressed.initCause(backend500);
    IOException closeWrapper = new IOException("close failed", backend500);
    fnfeWithWrappedTailSuppressed.addSuppressed(closeWrapper);
    assertThat(BatchErrorCollector.isNotFound(fnfeWithWrappedTailSuppressed)).isFalse();

    BatchErrorCollector c2 = BatchErrorCollector.ignoringNotFound();
    c2.onFailure(fnfeWithWrappedTailSuppressed);
    assertThat(c2.hasErrors()).isTrue();
    assertThat(c2.getFailureCount()).isEqualTo(1);
    assertThat(assertThrows(IOException.class, () -> c2.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(closeWrapper);

    IOException unclassified500 = new IOException("HTTP 500");
    FileNotFoundException fnfeWithRuntimeTailSuppressed =
        new FileNotFoundException("gs://bucket/missing-3");
    fnfeWithRuntimeTailSuppressed.initCause(unclassified500);
    RuntimeException runtimeClose = new RuntimeException("cleanup failed", unclassified500);
    fnfeWithRuntimeTailSuppressed.addSuppressed(runtimeClose);
    assertThat(BatchErrorCollector.isNotFound(fnfeWithRuntimeTailSuppressed)).isFalse();

    BatchErrorCollector c3 = BatchErrorCollector.ignoringNotFound();
    c3.onFailure(fnfeWithRuntimeTailSuppressed);
    assertThat(c3.hasErrors()).isTrue();
    assertThat(c3.getFailureCount()).isEqualTo(1);
    assertThat(assertThrows(IOException.class, () -> c3.throwIfFailed(OPERATION_SUMMARY)))
        .hasCauseThat()
        .isSameInstanceAs(runtimeClose);

    AccessDeniedException sharedDenied = new AccessDeniedException("gs://bucket/shared-secret");
    IOException inner500 = new IOException("inner 500");
    FileNotFoundException innerComposite404 = new FileNotFoundException("inner 404");
    innerComposite404.initCause(sharedDenied);
    innerComposite404.addSuppressed(inner500);

    FileNotFoundException outerComposite404 = new FileNotFoundException("outer 404");
    outerComposite404.addSuppressed(innerComposite404);
    outerComposite404.addSuppressed(sharedDenied);

    BatchErrorCollector c4 = BatchErrorCollector.ignoringNotFound();
    c4.onFailure(outerComposite404);
    assertThat(c4.getFailureCount()).isEqualTo(2);
    IOException thrownShared =
        assertThrows(IOException.class, () -> c4.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownShared).isSameInstanceAs(sharedDenied);
    assertThat(Arrays.asList(thrownShared.getSuppressed())).containsExactly(inner500);

    OutOfMemoryError causeOom = new OutOfMemoryError("cause OOM");
    IOException suppressed500 = new IOException("suppressed 500");
    FileNotFoundException fnfeWithOomCauseAndSuppressed =
        new FileNotFoundException("gs://bucket/missing-oom");
    fnfeWithOomCauseAndSuppressed.initCause(causeOom);
    fnfeWithOomCauseAndSuppressed.addSuppressed(suppressed500);
    assertThat(BatchErrorCollector.isNotFound(fnfeWithOomCauseAndSuppressed)).isFalse();

    BatchErrorCollector c5 = BatchErrorCollector.ignoringNotFound();
    c5.onFailure(fnfeWithOomCauseAndSuppressed);
    assertThat(c5.hasErrors()).isTrue();
    assertThat(assertThrows(OutOfMemoryError.class, () -> c5.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(causeOom);
  }

  @Test
  void
      throwIfFailed_transparentWrappersWithSuppressed_preservesWrapperSuppressedAndRethrowsVmError() {
    IOException innerIoe = new IOException("inner stream error");
    IOException closeSuppressed = new IOException("close suppressed error");
    UncheckedIOException uncheckedWithSuppressed = new UncheckedIOException(innerIoe);
    uncheckedWithSuppressed.addSuppressed(closeSuppressed);

    BatchErrorCollector singleWrapperCollector = BatchErrorCollector.create();
    singleWrapperCollector.onFailure(uncheckedWithSuppressed);
    IOException thrownSingle =
        assertThrows(
            IOException.class, () -> singleWrapperCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownSingle).hasCauseThat().isSameInstanceAs(uncheckedWithSuppressed);
    assertThat(Arrays.asList(thrownSingle.getCause().getSuppressed()))
        .containsExactly(closeSuppressed);

    IOException sharedIoe = new IOException("shared failure");
    IOException wrapperCloseError = new IOException("cleanup failure on wrapper");
    ExecutionException execWithSuppressed = new ExecutionException(sharedIoe);
    execWithSuppressed.addSuppressed(wrapperCloseError);

    BatchErrorCollector sharedFirstCollector = BatchErrorCollector.create();
    sharedFirstCollector.onFailure(sharedIoe);
    sharedFirstCollector.onFailure(execWithSuppressed);
    IOException thrownSharedFirst =
        assertThrows(
            IOException.class, () -> sharedFirstCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownSharedFirst).isSameInstanceAs(sharedIoe);
    assertThat(Arrays.asList(thrownSharedFirst.getSuppressed())).containsExactly(wrapperCloseError);

    BatchErrorCollector sharedWithPrimaryCollector = BatchErrorCollector.create();
    AccessDeniedException primaryDenied = new AccessDeniedException("gs://bucket/denied");
    sharedWithPrimaryCollector.onFailure(sharedIoe);
    sharedWithPrimaryCollector.onFailure(execWithSuppressed);
    sharedWithPrimaryCollector.onFailure(primaryDenied);
    IOException thrownWithPrimary =
        assertThrows(
            IOException.class, () -> sharedWithPrimaryCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownWithPrimary).isSameInstanceAs(primaryDenied);
    assertThat(thrownWithPrimary.getSuppressed()).hasLength(2);
    assertThat(thrownWithPrimary.getSuppressed()[0]).isSameInstanceAs(sharedIoe);
    assertThat(thrownWithPrimary.getSuppressed()[1])
        .hasCauseThat()
        .isSameInstanceAs(execWithSuppressed);

    OutOfMemoryError suppressedOom = new OutOfMemoryError("wrapper suppressed OOM");
    ExecutionException execWithOom = new ExecutionException(sharedIoe);
    execWithOom.addSuppressed(suppressedOom);

    BatchErrorCollector oomAlongsideSharedCollector = BatchErrorCollector.create();
    oomAlongsideSharedCollector.onFailure(sharedIoe);
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES + 10; i++) {
      oomAlongsideSharedCollector.onFailure(new IOException("filler-" + i));
    }
    oomAlongsideSharedCollector.onFailure(execWithOom);
    assertThat(
            assertThrows(
                OutOfMemoryError.class,
                () -> oomAlongsideSharedCollector.throwIfFailed(OPERATION_SUMMARY)))
        .isSameInstanceAs(suppressedOom);
  }

  @Test
  void
      onFailure_duplicateInstancesAboveMaxRetained_doesNotStarveDistinctErrorsOrInflateOmittedCount() {
    BatchErrorCollector sameOnlyCollector = BatchErrorCollector.create();
    IOException repeatedOnly = new IOException("shared channel failure");
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES + 50; i++) {
      sameOnlyCollector.onFailure(repeatedOnly);
    }
    assertThat(sameOnlyCollector.getFailureCount())
        .isEqualTo(BatchErrorCollector.MAX_RETAINED_FAILURES + 50);
    IOException thrownSameOnly =
        assertThrows(IOException.class, () -> sameOnlyCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownSameOnly).isSameInstanceAs(repeatedOnly);
    assertThat(thrownSameOnly.getSuppressed()).isEmpty();

    BatchErrorCollector mixedBurstCollector = BatchErrorCollector.create();
    AccessDeniedException sharedDenied = new AccessDeniedException("gs://bucket/shared-denied");
    for (int i = 0; i < 60; i++) {
      mixedBurstCollector.onFailure(sharedDenied);
      mixedBurstCollector.onFailure(new ExecutionException(sharedDenied));
      mixedBurstCollector.onFailure(new UncheckedIOException(sharedDenied));
    }
    AccessDeniedException distinctDenied = new AccessDeniedException("gs://bucket/other-denied");
    FileNotFoundException distinctMissing = new FileNotFoundException("gs://bucket/missing");
    IOException distinctGeneric = new IOException("distinct generic");
    mixedBurstCollector.onFailure(distinctDenied);
    mixedBurstCollector.onFailure(distinctMissing);
    mixedBurstCollector.onFailure(distinctGeneric);

    assertThat(mixedBurstCollector.getFailureCount()).isEqualTo(183);
    IOException thrownMixed =
        assertThrows(IOException.class, () -> mixedBurstCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownMixed).isSameInstanceAs(sharedDenied);
    assertThat(Arrays.asList(thrownMixed.getSuppressed()))
        .containsExactly(distinctDenied, distinctMissing, distinctGeneric)
        .inOrder();

    BatchErrorCollector hundredDistinctCollector = BatchErrorCollector.create();
    IOException first = new IOException("distinct-0");
    hundredDistinctCollector.onFailure(first);
    hundredDistinctCollector.onFailure(first);
    hundredDistinctCollector.onFailure(new CompletionException(first));
    for (int i = 1; i < BatchErrorCollector.MAX_RETAINED_FAILURES; i++) {
      hundredDistinctCollector.onFailure(new IOException("distinct-" + i));
    }
    assertThat(hundredDistinctCollector.getFailureCount())
        .isEqualTo(BatchErrorCollector.MAX_RETAINED_FAILURES + 2);
    IOException thrownHundred =
        assertThrows(
            IOException.class, () -> hundredDistinctCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownHundred).isSameInstanceAs(first);
    assertThat(thrownHundred.getSuppressed())
        .hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES - 1);
    for (Throwable s : thrownHundred.getSuppressed()) {
      assertThat(s.getMessage()).doesNotContain("more suppressed failures");
    }
  }

  @Test
  void
      throwIfFailed_cacheMissAboveMaxRetained_updatesOmittedSummaryInPlaceAndPreservesInnerSummary() {
    BatchErrorCollector collector = BatchErrorCollector.create();
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES + 1; i++) {
      collector.onFailure(new IOException("error-" + i));
    }

    IOException firstThrow =
        assertThrows(IOException.class, () -> collector.throwIfFailed("summary-1"));
    assertThat(firstThrow.getSuppressed()).hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES);
    assertThat(firstThrow.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES - 1])
        .hasMessageThat()
        .isEqualTo("... and 1 more suppressed failures");

    IOException differentSummaryThrow =
        assertThrows(IOException.class, () -> collector.throwIfFailed("summary-2"));
    assertThat(differentSummaryThrow).isSameInstanceAs(firstThrow);
    assertThat(differentSummaryThrow.getSuppressed())
        .hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES);
    assertThat(differentSummaryThrow.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES - 1])
        .hasMessageThat()
        .isEqualTo("... and 1 more suppressed failures");

    for (int i = 0; i < 4; i++) {
      collector.onFailure(new IOException("late-error-" + i));
    }
    IOException updatedCountThrow =
        assertThrows(IOException.class, () -> collector.throwIfFailed("summary-2"));
    assertThat(updatedCountThrow).isSameInstanceAs(firstThrow);
    assertThat(updatedCountThrow.getSuppressed())
        .hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES);
    assertThat(updatedCountThrow.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES - 1])
        .hasMessageThat()
        .isEqualTo("... and 5 more suppressed failures");
    for (int i = 0; i < BatchErrorCollector.MAX_RETAINED_FAILURES - 1; i++) {
      assertThat(updatedCountThrow.getSuppressed()[i].getMessage())
          .doesNotContain("more suppressed failures");
    }

    BatchErrorCollector outerCollector = BatchErrorCollector.create();
    outerCollector.onFailure(updatedCountThrow);
    outerCollector.onFailure(new IOException("outer-1"));
    outerCollector.onFailure(new IOException("outer-2"));

    IOException outerThrow1 =
        assertThrows(IOException.class, () -> outerCollector.throwIfFailed("outer-summary-1"));
    assertThat(outerThrow1).isSameInstanceAs(updatedCountThrow);
    assertThat(outerThrow1.getSuppressed())
        .hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES + 1);
    assertThat(outerThrow1.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES - 1])
        .hasMessageThat()
        .isEqualTo("... and 5 more suppressed failures");
    assertThat(outerThrow1.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES])
        .hasMessageThat()
        .isEqualTo("... and 2 more suppressed failures");

    outerCollector.onFailure(new IOException("outer-3"));
    IOException outerThrow2 =
        assertThrows(IOException.class, () -> outerCollector.throwIfFailed("outer-summary-2"));
    assertThat(outerThrow2.getSuppressed())
        .hasLength(BatchErrorCollector.MAX_RETAINED_FAILURES + 1);
    assertThat(outerThrow2.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES - 1])
        .hasMessageThat()
        .isEqualTo("... and 5 more suppressed failures");
    assertThat(outerThrow2.getSuppressed()[BatchErrorCollector.MAX_RETAINED_FAILURES])
        .hasMessageThat()
        .isEqualTo("... and 3 more suppressed failures");
  }

  @Test
  void throwIfFailed_wrappedNullFileDirNotEmptyAndEmptyOperationSummary_formatsCleanly() {
    BatchErrorCollector nullDirCollector = BatchErrorCollector.create();
    RuntimeException wrappedNullDir =
        new RuntimeException("delete failed", new DirectoryNotEmptyException(null));
    nullDirCollector.onFailure(wrappedNullDir);

    DirectoryNotEmptyException thrownDir =
        assertThrows(
            DirectoryNotEmptyException.class,
            () -> nullDirCollector.throwIfFailed(OPERATION_SUMMARY));
    assertThat(thrownDir.getFile()).isNull();
    assertThat(thrownDir.getMessage()).isNull();
    assertThat(thrownDir).hasCauseThat().isSameInstanceAs(wrappedNullDir);

    BatchErrorCollector emptySummary404 = BatchErrorCollector.create();
    emptySummary404.onFailure(new StorageException(404, "Not Found"));
    FileNotFoundException fnfe =
        assertThrows(FileNotFoundException.class, () -> emptySummary404.throwIfFailed(""));
    assertThat(fnfe).hasMessageThat().isEqualTo("Not Found");

    BatchErrorCollector emptySummary403 = BatchErrorCollector.create();
    emptySummary403.onFailure(new StorageException(403, "Forbidden"));
    AccessDeniedException ade =
        assertThrows(AccessDeniedException.class, () -> emptySummary403.throwIfFailed(""));
    assertThat(ade.getFile()).isNull();
    assertThat(ade.getReason()).isEqualTo("Forbidden");
    assertThat(ade.getMessage()).isEqualTo("Forbidden");

    BatchErrorCollector bothEmpty403 = BatchErrorCollector.create();
    bothEmpty403.onFailure(new StorageException(403, ""));
    AccessDeniedException emptyAde =
        assertThrows(AccessDeniedException.class, () -> bothEmpty403.throwIfFailed(""));
    assertThat(emptyAde.getMessage()).isEmpty();
  }

  @Test
  void nullArguments_throwNullPointerException() {
    BatchErrorCollector collector = BatchErrorCollector.create();

    assertThrows(NullPointerException.class, () -> collector.onFailure(null));
    assertThrows(NullPointerException.class, () -> collector.throwIfFailed(null));
  }
}
