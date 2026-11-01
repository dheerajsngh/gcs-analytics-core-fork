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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BatchExecutorTest {

  private BatchExecutor batchExecutor;

  @BeforeEach
  void setUp() {
    batchExecutor = new BatchExecutor(10);
  }

  @AfterEach
  void tearDown() {
    Thread.interrupted();
    try {
      batchExecutor.shutdown();
    } catch (IOException ignored) {
      // Expected in tests verifying failure propagation.
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void constructor_withNegativeNumThreads_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new BatchExecutor(-1));

    assertThat(exception).hasMessageThat().contains("numThreads must be non-negative");
  }

  @Test
  void constructor_withExcessiveNumThreads_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new BatchExecutor(GcsClientOptions.MAX_BATCH_THREADS + 1));

    assertThat(exception).hasMessageThat().contains("numThreads must be at most");
  }

  @Test
  void queue_nullTask_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> batchExecutor.queue(null, null));
  }

  @Test
  void queue_withDirectExecutor_executesSynchronouslyAndIsAlwaysIdle() throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    List<Boolean> results = new ArrayList<>();

    executor.queue(() -> true, recordCallback(results, null));
    executor.shutdown();

    assertThat(results).containsExactly(true);
    assertThat(executor.isIdle()).isTrue();
  }

  @Test
  void queue_withDirectExecutorAndPreInterruptedThread_completesTasksAndPreservesInterrupt()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    List<Integer> callbackResults = new ArrayList<>();
    AtomicInteger nullCallbackRuns = new AtomicInteger();

    Thread.currentThread().interrupt();
    try {
      executor.queue(() -> 1, recordCallback(callbackResults, null));
      executor.queue(
          () -> {
            nullCallbackRuns.incrementAndGet();
            return 2;
          },
          null);
      executor.awaitRequestsCompletion();

      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      executor.queue(() -> 3, recordCallback(callbackResults, null));
      executor.shutdown();

      assertThat(callbackResults).containsExactly(1, 3).inOrder();
      assertThat(nullCallbackRuns.get()).isEqualTo(1);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void awaitRequestsCompletion_withPreInterruptedCallerOnCompletedPoolTasks_preservesInterrupt()
      throws Exception {
    CountDownLatch allFinished = new CountDownLatch(4);
    List<Integer> results = new CopyOnWriteArrayList<>();

    for (int i = 0; i < 4; i++) {
      final int value = i;
      batchExecutor.queue(
          () -> {
            results.add(value);
            allFinished.countDown();
            return value;
          },
          null);
    }
    assertThat(allFinished.await(5, TimeUnit.SECONDS)).isTrue();
    // Wait briefly so FutureTask transitions state to COMPLETING -> NORMAL before get().
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!batchExecutor.isIdle() && System.nanoTime() < deadlineNanos) {
      Thread.yield();
    }

    Thread.currentThread().interrupt();
    try {
      batchExecutor.shutdown();

      assertThat(results).hasSize(4);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void queue_executesMultipleTasksConcurrently() throws Exception {
    List<Integer> results = new CopyOnWriteArrayList<>();

    for (int i = 0; i < 10; i++) {
      final int value = i;
      batchExecutor.queue(() -> value, recordCallback(results, null));
    }
    batchExecutor.shutdown();

    assertThat(results).hasSize(10);
  }

  @Test
  void queue_withContravariantCallback_acceptsSupertypeFutureCallback() throws Exception {
    List<Object> results = new CopyOnWriteArrayList<>();
    FutureCallback<Object> objectCallback = recordCallback(results, null);

    batchExecutor.queue(() -> "hello", objectCallback);
    batchExecutor.shutdown();

    assertThat(results).containsExactly("hello");
  }

  @Test
  void queue_callbackOnSuccessThrowsRuntimeException_doesNotCallOnFailure() {
    AtomicBoolean onSuccessCalled = new AtomicBoolean(false);
    AtomicBoolean onFailureCalled = new AtomicBoolean(false);
    FutureCallback<String> throwingCallback =
        new FutureCallback<String>() {
          @Override
          public void onSuccess(@Nullable String result) {
            onSuccessCalled.set(true);
            throw new IllegalStateException("onSuccess failure");
          }

          @Override
          public void onFailure(Throwable t) {
            onFailureCalled.set(true);
          }
        };

    batchExecutor.queue(() -> "ok", throwingCallback);
    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(onSuccessCalled.get()).isTrue();
    assertThat(onFailureCalled.get()).isFalse();
    assertThat(exception).hasMessageThat().isEqualTo("Batch task failed");
    assertThat(exception.getCause()).isInstanceOf(ExecutionException.class);
    assertThat(exception.getCause().getCause()).isInstanceOf(IllegalStateException.class);
    assertThat(exception.getCause().getCause()).hasMessageThat().isEqualTo("onSuccess failure");
  }

  @Test
  void queue_directExecutorOnSuccessThrowsRuntimeException_defersFailureWithoutCallingOnFailure() {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicBoolean onFailureCalled = new AtomicBoolean(false);
    FutureCallback<String> throwingCallback =
        new FutureCallback<String>() {
          @Override
          public void onSuccess(@Nullable String result) {
            throw new IllegalArgumentException("direct onSuccess failure");
          }

          @Override
          public void onFailure(Throwable t) {
            onFailureCalled.set(true);
          }
        };

    executor.queue(() -> "ok", throwingCallback);
    assertThat(onFailureCalled.get()).isFalse();

    IOException exception = assertThrows(IOException.class, executor::shutdown);

    assertThat(onFailureCalled.get()).isFalse();
    assertThat(exception.getCause()).isInstanceOf(ExecutionException.class);
    assertThat(exception.getCause().getCause())
        .hasMessageThat()
        .isEqualTo("direct onSuccess failure");
  }

  @Test
  void queue_taskThrowsException_callsOnFailureWhenCallbackProvided() throws Exception {
    SettableFuture<Throwable> failure = SettableFuture.create();

    batchExecutor.queue(
        () -> {
          throw new IOException("simulated failure");
        },
        recordCallback(null, failure));
    batchExecutor.shutdown();

    assertThat(failure.get()).isInstanceOf(IOException.class);
    assertThat(failure.get()).hasMessageThat().isEqualTo("simulated failure");
  }

  @Test
  void queue_taskThrowsInterruptedExceptionWithCallback_restoresInterruptBeforeOnFailure()
      throws Exception {
    AtomicBoolean interruptedDuringOnFailure = new AtomicBoolean(false);
    SettableFuture<Throwable> capturedFailure = SettableFuture.create();

    batchExecutor.queue(
        () -> {
          throw new InterruptedException("worker interrupted");
        },
        new FutureCallback<Object>() {
          @Override
          public void onSuccess(@Nullable Object result) {}

          @Override
          public void onFailure(Throwable t) {
            interruptedDuringOnFailure.set(Thread.currentThread().isInterrupted());
            capturedFailure.set(t);
          }
        });
    batchExecutor.shutdown();

    assertThat(capturedFailure.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
    assertThat(interruptedDuringOnFailure.get()).isTrue();
  }

  @Test
  void queue_taskThrowsInterruptedExceptionWithoutCallback_throwsInterruptedIOException() {
    try {
      batchExecutor.queue(
          () -> {
            throw new InterruptedException("task interrupted");
          },
          null);

      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, batchExecutor::shutdown);

      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(exception.getCause()).hasMessageThat().isEqualTo("task interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void queue_taskThrowsError_notifiesCallbackAndPropagatesOnShutdown() throws Exception {
    SettableFuture<Throwable> failure = SettableFuture.create();

    batchExecutor.queue(
        () -> {
          throw new AssertionError("fatal error");
        },
        recordCallback(null, failure));

    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(failure.get()).isInstanceOf(AssertionError.class);
    assertThat(exception.getCause()).isInstanceOf(ExecutionException.class);
    assertThat(exception.getCause().getCause()).isInstanceOf(AssertionError.class);
  }

  @Test
  void queue_taskThrowsException_propagateOnShutdownWhenCallbackIsNull() {
    batchExecutor.queue(
        () -> {
          throw new IOException("simulated failure");
        },
        null);

    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(exception).hasMessageThat().isEqualTo("simulated failure");
  }

  @Test
  void queue_taskThrowsRuntimeException_wrapsInIOExceptionOnShutdown() {
    batchExecutor.queue(
        () -> {
          throw new RuntimeException("runtime error");
        },
        null);

    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(exception).hasMessageThat().isEqualTo("Batch task failed");
    assertThat(exception.getCause()).isInstanceOf(ExecutionException.class);
    assertThat(exception.getCause().getCause()).hasMessageThat().isEqualTo("runtime error");
  }

  @Test
  void queue_afterShutdown_throwsIllegalStateException() throws Exception {
    batchExecutor.shutdown();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> batchExecutor.queue(() -> "task", null));

    assertThat(exception)
        .hasMessageThat()
        .startsWith("requestExecutor should not be terminated to queue request");
  }

  @Test
  void isIdle_forThreadPoolExecutor_returnsCorrectState() throws Exception {
    SettableFuture<Void> taskStarted = SettableFuture.create();
    SettableFuture<Void> taskShouldFinish = SettableFuture.create();

    assertThat(batchExecutor.isIdle()).isTrue();

    batchExecutor.queue(
        () -> {
          taskStarted.set(null);
          taskShouldFinish.get(5, TimeUnit.SECONDS);
          return null;
        },
        null);
    taskStarted.get(5, TimeUnit.SECONDS);

    assertThat(batchExecutor.isIdle()).isFalse();

    taskShouldFinish.set(null);
    batchExecutor.shutdown();

    assertThat(batchExecutor.isIdle()).isTrue();
  }

  @Test
  void queue_whenQueueSaturated_executesOnCallerThread() throws Exception {
    // Queue capacity = 1 thread * 20 tasks can be queued per thread = 20.
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch taskBlocker = new CountDownLatch(1);
    AtomicReference<Thread> saturatedTaskThread = new AtomicReference<>();

    try {
      for (int i = 0; i < 21; i++) {
        executor.queue(
            () -> {
              taskBlocker.await();
              return null;
            },
            null);
      }

      executor.queue(
          () -> {
            saturatedTaskThread.set(Thread.currentThread());
            return null;
          },
          null);

      assertThat(saturatedTaskThread.get()).isEqualTo(Thread.currentThread());
    } finally {
      taskBlocker.countDown();
      executor.shutdown();
    }
  }

  @Test
  void queue_overTenThousandTasks_keepsResponseFuturesBounded() throws Exception {
    int numThreads = 4;
    int totalTasks = 15_000;
    BatchExecutor executor = new BatchExecutor(numThreads);
    AtomicInteger completedTasks = new AtomicInteger();
    int maxObservedPending = 0;

    try {
      for (int i = 0; i < totalTasks; i++) {
        executor.queue(completedTasks::incrementAndGet, null);
        maxObservedPending = Math.max(maxObservedPending, executor.getPendingFuturesCount());
      }
      executor.awaitRequestsCompletion();

      assertThat(completedTasks.get()).isEqualTo(totalTasks);
      assertThat(maxObservedPending).isLessThan(numThreads * 22);
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  void queue_overTenThousandTasksWithDirectExecutor_keepsResponseFuturesAtZero() throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicInteger completedTasks = new AtomicInteger();
    int maxObservedPending = 0;

    try {
      for (int i = 0; i < 12_000; i++) {
        executor.queue(completedTasks::incrementAndGet, null);
        maxObservedPending = Math.max(maxObservedPending, executor.getPendingFuturesCount());
      }

      assertThat(completedTasks.get()).isEqualTo(12_000);
      assertThat(maxObservedPending).isEqualTo(0);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  void queue_manyHarvestedFailures_defersThrowAndCapsSuppressedExceptions() throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    IOException duplicateSharedFailure = new IOException("shared failure");

    try {
      // Submit shared failure multiple times to verify primary != other / deduplication guard.
      executor.queue(
          () -> {
            throw duplicateSharedFailure;
          },
          null);
      executor.queue(
          () -> {
            throw duplicateSharedFailure;
          },
          null);

      // Queue >10,000 tasks including 150 distinct failures so harvesting occurs during queue().
      for (int i = 0; i < 10_500; i++) {
        final int index = i;
        executor.queue(
            () -> {
              if (index < 150) {
                throw new IOException("failure-" + index);
              }
              return index;
            },
            null);
      }

      IOException thrown = assertThrows(IOException.class, executor::awaitRequestsCompletion);

      assertThat(thrown.getSuppressed()).hasLength(BatchExecutor.MAX_SUPPRESSED);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  void awaitRequestsCompletion_multiPhaseWhenClean_allowsSubsequentPhases() throws Exception {
    AtomicInteger phase1Count = new AtomicInteger();
    AtomicInteger phase2Count = new AtomicInteger();
    AtomicInteger phase3Count = new AtomicInteger();

    for (int i = 0; i < 25; i++) {
      batchExecutor.queue(phase1Count::incrementAndGet, null);
    }
    batchExecutor.awaitRequestsCompletion();
    assertThat(phase1Count.get()).isEqualTo(25);

    for (int i = 0; i < 25; i++) {
      batchExecutor.queue(
          () -> {
            assertThat(phase1Count.get()).isEqualTo(25);
            return phase2Count.incrementAndGet();
          },
          null);
    }
    batchExecutor.awaitRequestsCompletion();
    assertThat(phase2Count.get()).isEqualTo(25);

    for (int i = 0; i < 10; i++) {
      batchExecutor.queue(phase3Count::incrementAndGet, null);
    }
    batchExecutor.shutdown();
    assertThat(phase3Count.get()).isEqualTo(10);
  }

  @Test
  void awaitRequestsCompletion_drainsAllSiblingTasksBeforeThrowingAndResetsForNextPhase()
      throws Exception {
    CountDownLatch fastFailureEntered = new CountDownLatch(1);
    AtomicInteger completedSiblings = new AtomicInteger();

    batchExecutor.queue(
        () -> {
          fastFailureEntered.countDown();
          throw new IOException("phase-1 primary failure");
        },
        null);

    for (int i = 0; i < 5; i++) {
      final int index = i;
      batchExecutor.queue(
          () -> {
            assertThat(fastFailureEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Uninterruptibles.sleepUninterruptibly(40, TimeUnit.MILLISECONDS);
            completedSiblings.incrementAndGet();
            if (index == 2) {
              throw new IOException("phase-1 secondary failure");
            }
            return null;
          },
          null);
    }

    IOException phase1Exception =
        assertThrows(IOException.class, batchExecutor::awaitRequestsCompletion);

    // Every sibling task must have finished before awaitRequestsCompletion() threw.
    assertThat(completedSiblings.get()).isEqualTo(5);
    assertThat(phase1Exception).hasMessageThat().isEqualTo("phase-1 primary failure");
    assertThat(phase1Exception.getSuppressed()).hasLength(1);
    assertThat(phase1Exception.getSuppressed()[0])
        .hasMessageThat()
        .isEqualTo("phase-1 secondary failure");

    // Subsequent clean phase and shutdown must succeed without re-throwing phase-1 failure.
    AtomicInteger phase2Completed = new AtomicInteger();
    for (int i = 0; i < 5; i++) {
      batchExecutor.queue(phase2Completed::incrementAndGet, null);
    }
    batchExecutor.awaitRequestsCompletion();
    assertThat(phase2Completed.get()).isEqualTo(5);

    batchExecutor.shutdown();
  }

  @Test
  void shutdown_whenCallerInterruptedWhileBlocked_cancelsTasksRestoresInterruptAndWaitsGracePeriod()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch workerInterrupted = new CountDownLatch(1);
    AtomicBoolean workerGracefulCleanupFinished = new AtomicBoolean(false);
    AtomicBoolean queuedSiblingRan = new AtomicBoolean(false);

    executor.queue(
        () -> {
          workerStarted.countDown();
          try {
            new CountDownLatch(1).await();
          } catch (InterruptedException e) {
            workerInterrupted.countDown();
            // Simulate short post-cancellation cleanup well within the 1s shutdown grace period.
            Uninterruptibles.sleepUninterruptibly(80, TimeUnit.MILLISECONDS);
            workerGracefulCleanupFinished.set(true);
            throw e;
          }
          return null;
        },
        null);

    // Queued behind the single active worker; should be cancelled before running.
    executor.queue(
        () -> {
          queuedSiblingRan.set(true);
          return null;
        },
        null);

    assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
              callerThread.interrupt();
            });
    interrupter.setDaemon(true);
    interrupter.start();

    try {
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::shutdown);

      assertThat(Thread.interrupted()).isTrue();
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(workerInterrupted.getCount()).isEqualTo(0);
      assertThat(workerGracefulCleanupFinished.get()).isTrue();
      assertThat(queuedSiblingRan.get()).isFalse();
    } finally {
      Thread.interrupted();
      interrupter.join(5_000);
    }
  }

  @Test
  void awaitRequestsCompletion_whenCallerInterrupted_cancelsRemainingTasksAndPreservesInterrupt()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch workerInterrupted = new CountDownLatch(1);
    AtomicBoolean queuedSiblingRan = new AtomicBoolean(false);

    executor.queue(
        () -> {
          workerStarted.countDown();
          try {
            new CountDownLatch(1).await();
          } catch (InterruptedException e) {
            workerInterrupted.countDown();
            throw e;
          }
          return null;
        },
        null);
    executor.queue(
        () -> {
          queuedSiblingRan.set(true);
          return null;
        },
        null);

    assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
              callerThread.interrupt();
            });
    interrupter.setDaemon(true);
    interrupter.start();

    try {
      try {
        InterruptedIOException exception =
            assertThrows(InterruptedIOException.class, executor::awaitRequestsCompletion);

        assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
      } finally {
        // Call shutdown() while caller thread is STILL interrupted to verify shutdown() finally
        // handles a pre-existing interrupt from awaitRequestsCompletion().
        executor.shutdown();
      }
      assertThat(Thread.interrupted()).isTrue();
      assertThat(workerInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(queuedSiblingRan.get()).isFalse();
    } finally {
      Thread.interrupted();
      interrupter.join(5_000);
    }
  }

  @Test
  void
      awaitRequestsCompletion_workerThrowsInterruptedException_drainsSiblingsAndPreservesInterrupt()
          throws Exception {
    CountDownLatch interruptedTaskDone = new CountDownLatch(1);
    AtomicBoolean siblingFinished = new AtomicBoolean(false);

    batchExecutor.queue(
        () -> {
          interruptedTaskDone.countDown();
          throw new InterruptedException("worker task interrupted");
        },
        null);
    batchExecutor.queue(
        () -> {
          assertThat(interruptedTaskDone.await(5, TimeUnit.SECONDS)).isTrue();
          Uninterruptibles.sleepUninterruptibly(40, TimeUnit.MILLISECONDS);
          siblingFinished.set(true);
          return null;
        },
        null);

    try {
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, batchExecutor::awaitRequestsCompletion);

      assertThat(Thread.interrupted()).isTrue();
      assertThat(siblingFinished.get()).isTrue();
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
    } finally {
      Thread.interrupted();
      batchExecutor.shutdown();
    }
  }

  @Test
  void queue_callbackOnFailureThrowsRuntimeException_preservesOriginalExceptionAsSuppressed() {
    IOException taskFailure = new IOException("underlying task failure");
    IllegalStateException callbackFailure = new IllegalStateException("onFailure boom");

    batchExecutor.queue(
        () -> {
          throw taskFailure;
        },
        new FutureCallback<Object>() {
          @Override
          public void onSuccess(@Nullable Object result) {}

          @Override
          public void onFailure(Throwable t) {
            throw callbackFailure;
          }
        });

    IOException exception = assertThrows(IOException.class, batchExecutor::shutdown);

    assertThat(exception).hasMessageThat().isEqualTo("Batch task failed");
    assertThat(exception.getCause()).isInstanceOf(ExecutionException.class);
    assertThat(exception.getCause().getCause()).isSameInstanceAs(callbackFailure);
    assertThat(callbackFailure.getSuppressed()).asList().containsExactly(taskFailure);
  }

  @Test
  void
      queue_directExecutorTaskThrowsInterruptedExceptionWithoutCallback_doesNotLeakInterruptOrDropRestore()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicInteger siblingCompleted = new AtomicInteger();

    try {
      executor.queue(
          () -> {
            throw new InterruptedException("direct task interrupted");
          },
          null);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return siblingCompleted.incrementAndGet();
          },
          null);
      executor.queue(
          () -> {
            Thread.interrupted();
            return siblingCompleted.incrementAndGet();
          },
          null);

      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::shutdown);

      assertThat(siblingCompleted.get()).isEqualTo(2);
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(exception.getCause()).hasMessageThat().isEqualTo("direct task interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void queue_directExecutorTaskSelfInterruptsAndThrowsIOException_defersInterruptUntilAwait()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicInteger siblingCompleted = new AtomicInteger();

    try {
      executor.queue(
          () -> {
            Thread.currentThread().interrupt();
            throw new IOException("self-interrupted io");
          },
          null);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return siblingCompleted.incrementAndGet();
          },
          null);

      IOException exception = assertThrows(IOException.class, executor::awaitRequestsCompletion);

      assertThat(siblingCompleted.get()).isEqualTo(1);
      assertThat(exception).hasMessageThat().isEqualTo("self-interrupted io");
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void
      queue_callerRunsTaskThrowsInterruptedExceptionBelowHarvestThreshold_drainsSiblingsAndRestoresInterrupt()
          throws Exception {
    // 2 threads + 40 queue slots = 42 capacity; harvestThreshold = 44, so task 43 runs on caller
    // thread below harvestThreshold.
    BatchExecutor executor = new BatchExecutor(2);
    CountDownLatch workersActive = new CountDownLatch(2);
    CountDownLatch releaseWorkers = new CountDownLatch(1);
    AtomicInteger pooledSiblingsCompleted = new AtomicInteger();
    AtomicInteger callerRunsSiblingsCompleted = new AtomicInteger();
    AtomicReference<Thread> task43Thread = new AtomicReference<>();

    try {
      for (int i = 0; i < 42; i++) {
        final boolean isInitialWorker = i < 2;
        executor.queue(
            () -> {
              if (isInitialWorker) {
                workersActive.countDown();
              }
              assertThat(releaseWorkers.await(5, TimeUnit.SECONDS)).isTrue();
              Uninterruptibles.sleepUninterruptibly(15, TimeUnit.MILLISECONDS);
              return pooledSiblingsCompleted.incrementAndGet();
            },
            null);
      }
      assertThat(workersActive.await(5, TimeUnit.SECONDS)).isTrue();

      executor.queue(
          () -> {
            task43Thread.set(Thread.currentThread());
            throw new InterruptedException("caller-runs interrupted");
          },
          null);
      assertThat(task43Thread.get()).isEqualTo(Thread.currentThread());
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return callerRunsSiblingsCompleted.incrementAndGet();
          },
          null);
      executor.queue(
          () -> {
            Thread.interrupted();
            return callerRunsSiblingsCompleted.incrementAndGet();
          },
          null);

      releaseWorkers.countDown();
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::awaitRequestsCompletion);

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(callerRunsSiblingsCompleted.get()).isEqualTo(2);
      assertThat(pooledSiblingsCompleted.get()).isEqualTo(42);
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(exception.getCause()).hasMessageThat().isEqualTo("caller-runs interrupted");
    } finally {
      releaseWorkers.countDown();
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void queue_callerRunsSelfInterruptingTaskWithoutHarvest_doesNotCancelInFlightSiblingsOnAwait()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    CountDownLatch workersActive = new CountDownLatch(2);
    CountDownLatch releaseWorkers = new CountDownLatch(1);
    AtomicInteger pooledSiblingsCompleted = new AtomicInteger();

    try {
      for (int i = 0; i < 42; i++) {
        final boolean isInitialWorker = i < 2;
        executor.queue(
            () -> {
              if (isInitialWorker) {
                workersActive.countDown();
              }
              assertThat(releaseWorkers.await(5, TimeUnit.SECONDS)).isTrue();
              Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
              return pooledSiblingsCompleted.incrementAndGet();
            },
            null);
      }
      assertThat(workersActive.await(5, TimeUnit.SECONDS)).isTrue();

      // Task 43 runs on caller thread with responseFutures.size() == 43 (< harvestThreshold 44).
      executor.queue(
          () -> {
            Thread.currentThread().interrupt();
            throw new InterruptedException("self-interrupted caller-runs");
          },
          null);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      releaseWorkers.countDown();
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::awaitRequestsCompletion);

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(pooledSiblingsCompleted.get()).isEqualTo(42);
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
    } finally {
      releaseWorkers.countDown();
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void
      awaitRequestsCompletion_callerInterruptedAfterPriorTaskIOException_throwsInterruptedIOExceptionWithPriorSuppressed()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    IOException priorFailure = new IOException("prior task failure");
    CountDownLatch task1ReadyToThrow = new CountDownLatch(1);
    CountDownLatch task2Started = new CountDownLatch(1);

    executor.queue(
        () -> {
          task1ReadyToThrow.countDown();
          throw priorFailure;
        },
        null);
    executor.queue(
        () -> {
          task2Started.countDown();
          new CountDownLatch(1).await();
          return null;
        },
        null);

    assertThat(task1ReadyToThrow.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(task2Started.await(5, TimeUnit.SECONDS)).isTrue();
    Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);

    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
              callerThread.interrupt();
            });
    interrupter.setDaemon(true);
    interrupter.start();

    try {
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::awaitRequestsCompletion);

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(exception.getSuppressed()).asList().containsExactly(priorFailure);
    } finally {
      Thread.interrupted();
      executor.shutdown();
      interrupter.join(5_000);
    }
  }

  @Test
  void shutdown_callerInterruptedAfterMaxSuppressedFailures_stillThrowsInterruptedIOException()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    int failingTasks = BatchExecutor.MAX_SUPPRESSED + 20;
    CountDownLatch allFailuresEntered = new CountDownLatch(failingTasks);
    CountDownLatch blockerStarted = new CountDownLatch(1);

    for (int i = 0; i < failingTasks; i++) {
      final int index = i;
      executor.queue(
          () -> {
            allFailuresEntered.countDown();
            throw new IOException("prior-failure-" + index);
          },
          null);
    }
    assertThat(allFailuresEntered.await(5, TimeUnit.SECONDS)).isTrue();

    executor.queue(
        () -> {
          blockerStarted.countDown();
          new CountDownLatch(1).await();
          return null;
        },
        null);
    assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
              callerThread.interrupt();
            });
    interrupter.setDaemon(true);
    interrupter.start();

    try {
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::shutdown);

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(exception.getSuppressed()).hasLength(1);
      assertThat(exception.getSuppressed()[0]).isInstanceOf(IOException.class);
      assertThat(exception.getSuppressed()[0].getSuppressed())
          .hasLength(BatchExecutor.MAX_SUPPRESSED);
    } finally {
      Thread.interrupted();
      interrupter.join(5_000);
    }
  }

  @Test
  void
      queue_directExecutorTaskThrowsInterruptedExceptionWithCallback_defersInterruptAndProtectsSiblings()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicBoolean interruptedInOnFailure = new AtomicBoolean(false);
    AtomicReference<Throwable> capturedFailure = new AtomicReference<>();
    List<Integer> siblingSuccesses = new ArrayList<>();
    List<Throwable> siblingFailures = new ArrayList<>();

    try {
      executor.queue(
          () -> {
            throw new InterruptedException("direct callback interrupted");
          },
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {}

            @Override
            public void onFailure(Throwable t) {
              interruptedInOnFailure.set(Thread.currentThread().isInterrupted());
              capturedFailure.set(t);
            }
          });

      assertThat(interruptedInOnFailure.get()).isTrue();
      assertThat(capturedFailure.get()).isInstanceOf(InterruptedException.class);
      assertThat(capturedFailure.get()).hasMessageThat().isEqualTo("direct callback interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return 1;
          },
          recordingListCallback(siblingSuccesses, siblingFailures));
      executor.queue(
          () -> {
            Thread.interrupted();
            return 2;
          },
          recordingListCallback(siblingSuccesses, siblingFailures));

      executor.awaitRequestsCompletion();

      assertThat(siblingFailures).isEmpty();
      assertThat(siblingSuccesses).containsExactly(1, 2).inOrder();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      Thread.interrupted();
      executor.queue(() -> 3, recordingListCallback(siblingSuccesses, siblingFailures));
      executor.shutdown();

      assertThat(siblingFailures).isEmpty();
      assertThat(siblingSuccesses).containsExactly(1, 2, 3).inOrder();
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
    } finally {
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void
      queue_directExecutorTaskSelfInterruptsAndThrowsIOExceptionWithCallback_defersInterruptUntilShutdown()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicBoolean interruptedInOnFailure = new AtomicBoolean(false);
    AtomicReference<Throwable> capturedFailure = new AtomicReference<>();
    List<Integer> siblingSuccesses = new ArrayList<>();
    List<Throwable> siblingFailures = new ArrayList<>();

    try {
      executor.queue(
          () -> {
            Thread.currentThread().interrupt();
            throw new IOException("gax self-interrupted io");
          },
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {}

            @Override
            public void onFailure(Throwable t) {
              interruptedInOnFailure.set(Thread.currentThread().isInterrupted());
              capturedFailure.set(t);
            }
          });

      assertThat(interruptedInOnFailure.get()).isTrue();
      assertThat(capturedFailure.get()).isInstanceOf(IOException.class);
      assertThat(capturedFailure.get()).hasMessageThat().isEqualTo("gax self-interrupted io");
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return 1;
          },
          recordingListCallback(siblingSuccesses, siblingFailures));

      executor.shutdown();

      assertThat(siblingFailures).isEmpty();
      assertThat(siblingSuccesses).containsExactly(1);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void
      queue_directExecutorTaskCatchesInterruptedExceptionRestoresInterruptAndReturnsFallback_defersInterruptUntilAwait()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    AtomicBoolean interruptedInOnSuccess = new AtomicBoolean(false);
    List<String> results = new ArrayList<>();
    List<Throwable> failures = new ArrayList<>();

    try {
      executor.queue(
          () -> {
            try {
              throw new InterruptedException("caught internally");
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return "fallback";
            }
          },
          new FutureCallback<String>() {
            @Override
            public void onSuccess(@Nullable String result) {
              interruptedInOnSuccess.set(Thread.currentThread().isInterrupted());
              results.add(result);
            }

            @Override
            public void onFailure(Throwable t) {
              failures.add(t);
            }
          });

      assertThat(interruptedInOnSuccess.get()).isTrue();
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return "sibling";
          },
          recordingListCallback(results, failures));

      executor.awaitRequestsCompletion();

      assertThat(failures).isEmpty();
      assertThat(results).containsExactly("fallback", "sibling").inOrder();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void
      queue_callerRunsTaskThrowsInterruptedExceptionWithCallback_drainsPooledSiblingsWithoutCancellationAndRestoresInterrupt()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    CountDownLatch workersActive = new CountDownLatch(2);
    CountDownLatch releaseWorkers = new CountDownLatch(1);
    List<Integer> pooledSuccesses = new CopyOnWriteArrayList<>();
    List<Throwable> pooledFailures = new CopyOnWriteArrayList<>();
    List<Integer> callerRunsSuccesses = new CopyOnWriteArrayList<>();
    List<Throwable> callerRunsFailures = new CopyOnWriteArrayList<>();
    AtomicReference<Thread> task43Thread = new AtomicReference<>();
    AtomicBoolean interruptedInOnFailure = new AtomicBoolean(false);
    AtomicReference<Throwable> capturedFailure = new AtomicReference<>();

    try {
      for (int i = 0; i < 42; i++) {
        final int taskId = i;
        final boolean isInitialWorker = i < 2;
        executor.queue(
            () -> {
              if (isInitialWorker) {
                workersActive.countDown();
              }
              assertThat(releaseWorkers.await(5, TimeUnit.SECONDS)).isTrue();
              Uninterruptibles.sleepUninterruptibly(15, TimeUnit.MILLISECONDS);
              return taskId;
            },
            recordingListCallback(pooledSuccesses, pooledFailures));
      }
      assertThat(workersActive.await(5, TimeUnit.SECONDS)).isTrue();

      executor.queue(
          () -> {
            task43Thread.set(Thread.currentThread());
            throw new InterruptedException("caller-runs callback interrupted");
          },
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {}

            @Override
            public void onFailure(Throwable t) {
              interruptedInOnFailure.set(Thread.currentThread().isInterrupted());
              capturedFailure.set(t);
            }
          });

      assertThat(task43Thread.get()).isEqualTo(Thread.currentThread());
      assertThat(interruptedInOnFailure.get()).isTrue();
      assertThat(capturedFailure.get()).isInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return 101;
          },
          recordingListCallback(callerRunsSuccesses, callerRunsFailures));
      executor.queue(
          () -> {
            Thread.interrupted();
            return 102;
          },
          recordingListCallback(callerRunsSuccesses, callerRunsFailures));

      releaseWorkers.countDown();
      executor.awaitRequestsCompletion();

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(pooledFailures).isEmpty();
      assertThat(pooledSuccesses).hasSize(42);
      assertThat(callerRunsFailures).isEmpty();
      assertThat(callerRunsSuccesses).containsExactly(101, 102).inOrder();

      executor.shutdown();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      releaseWorkers.countDown();
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void
      queue_callerRunsTaskSelfInterruptsAndThrowsIOExceptionWithCallback_drainsPooledSiblingsAndRestoresInterruptOnShutdown()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    CountDownLatch workersActive = new CountDownLatch(2);
    CountDownLatch releaseWorkers = new CountDownLatch(1);
    List<Integer> pooledSuccesses = new CopyOnWriteArrayList<>();
    List<Throwable> pooledFailures = new CopyOnWriteArrayList<>();
    List<Integer> callerRunsSuccesses = new CopyOnWriteArrayList<>();
    List<Throwable> callerRunsFailures = new CopyOnWriteArrayList<>();
    AtomicBoolean interruptedInOnFailure = new AtomicBoolean(false);
    AtomicReference<Throwable> capturedFailure = new AtomicReference<>();

    try {
      for (int i = 0; i < 42; i++) {
        final int taskId = i;
        final boolean isInitialWorker = i < 2;
        executor.queue(
            () -> {
              if (isInitialWorker) {
                workersActive.countDown();
              }
              assertThat(releaseWorkers.await(5, TimeUnit.SECONDS)).isTrue();
              Uninterruptibles.sleepUninterruptibly(15, TimeUnit.MILLISECONDS);
              return taskId;
            },
            recordingListCallback(pooledSuccesses, pooledFailures));
      }
      assertThat(workersActive.await(5, TimeUnit.SECONDS)).isTrue();

      executor.queue(
          () -> {
            Thread.currentThread().interrupt();
            throw new IOException("caller-runs gax io");
          },
          new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {}

            @Override
            public void onFailure(Throwable t) {
              interruptedInOnFailure.set(Thread.currentThread().isInterrupted());
              capturedFailure.set(t);
            }
          });

      assertThat(interruptedInOnFailure.get()).isTrue();
      assertThat(capturedFailure.get()).isInstanceOf(IOException.class);
      assertThat(capturedFailure.get()).hasMessageThat().isEqualTo("caller-runs gax io");
      assertThat(Thread.currentThread().isInterrupted()).isFalse();

      executor.queue(
          () -> {
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            return 201;
          },
          recordingListCallback(callerRunsSuccesses, callerRunsFailures));

      releaseWorkers.countDown();
      executor.shutdown();

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(pooledFailures).isEmpty();
      assertThat(pooledSuccesses).hasSize(42);
      assertThat(callerRunsFailures).isEmpty();
      assertThat(callerRunsSuccesses).containsExactly(201);
    } finally {
      releaseWorkers.countDown();
      Thread.interrupted();
      executor.shutdown();
    }
  }

  @Test
  void newRequestExecutor_whenAlreadyShutdown_rejectedExecutionThrowsIllegalStateException() {
    ThreadPoolExecutor pool = BatchExecutor.newRequestExecutor(1);
    pool.shutdown();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> pool.submit(() -> null));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("requestExecutor should not be terminated to queue request");
  }

  @Test
  void queue_concurrentWithShutdown_neverLeavesUncompletedFutureInResponseFutures()
      throws Exception {
    for (int round = 0; round < 20; round++) {
      final boolean saturateQueue = (round % 2 == 1);
      BatchExecutor executor = new BatchExecutor(1);
      // Warm up worker thread so cold-start thread creation does not skew the queue/shutdown race.
      executor.queue(() -> -1, null);
      executor.awaitRequestsCompletion();

      CountDownLatch startRace = new CountDownLatch(1);
      CountDownLatch firstQueued = new CountDownLatch(1);
      AtomicInteger acceptedTasks = new AtomicInteger();
      AtomicInteger executedTasks = new AtomicInteger();
      AtomicReference<Throwable> unexpectedProducerError = new AtomicReference<>();

      Thread producer =
          new Thread(
              () -> {
                Uninterruptibles.awaitUninterruptibly(startRace);
                try {
                  for (int i = 0; i < 200; i++) {
                    final int index = i;
                    try {
                      executor.queue(
                          () -> {
                            if (saturateQueue && index < 25) {
                              LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
                            }
                            return executedTasks.incrementAndGet();
                          },
                          null);
                      acceptedTasks.incrementAndGet();
                      if ((!saturateQueue && i == 0) || (saturateQueue && i == 21)) {
                        firstQueued.countDown();
                      }
                    } catch (IllegalStateException expectedAfterShutdown) {
                      assertThat(expectedAfterShutdown)
                          .hasMessageThat()
                          .contains("requestExecutor should not be terminated to queue request");
                      return;
                    } catch (Throwable t) {
                      unexpectedProducerError.set(t);
                      return;
                    }
                  }
                } finally {
                  firstQueued.countDown();
                }
              });
      producer.setDaemon(true);
      producer.start();

      startRace.countDown();
      assertThat(firstQueued.await(5, TimeUnit.SECONDS)).isTrue();
      executor.shutdown();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);

      producer.join(5_000);

      assertThat(producer.isAlive()).isFalse();
      assertThat(unexpectedProducerError.get()).isNull();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
      assertThat(acceptedTasks.get()).isAtLeast(1);
      assertThat(executedTasks.get()).isEqualTo(acceptedTasks.get());

      // Idempotent shutdown() must return immediately rather than hanging on a dropped FutureTask.
      executor.shutdown();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
    }
  }

  @Test
  void shutdown_withInFlightConcurrentCallerRunsFailure_drainsAndPropagatesFailure()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch workerActive = new CountDownLatch(1);
    CountDownLatch releasePool = new CountDownLatch(1);
    CountDownLatch callerRunsEntered = new CountDownLatch(1);
    CountDownLatch releaseCallerRuns = new CountDownLatch(1);
    AtomicReference<Throwable> producerError = new AtomicReference<>();
    AtomicReference<Throwable> shutdownError = new AtomicReference<>();

    for (int i = 0; i < 21; i++) {
      final boolean isFirst = (i == 0);
      executor.queue(
          () -> {
            if (isFirst) {
              workerActive.countDown();
            }
            assertThat(releasePool.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
          },
          null);
    }
    assertThat(workerActive.await(5, TimeUnit.SECONDS)).isTrue();

    Thread producer =
        new Thread(
            () -> {
              try {
                executor.queue(
                    () -> {
                      callerRunsEntered.countDown();
                      assertThat(releaseCallerRuns.await(5, TimeUnit.SECONDS)).isTrue();
                      throw new IOException("in-flight caller-runs failure");
                    },
                    null);
              } catch (Throwable t) {
                producerError.set(t);
              }
            });
    producer.setDaemon(true);
    producer.start();
    assertThat(callerRunsEntered.await(5, TimeUnit.SECONDS)).isTrue();

    Thread shutdownThread =
        new Thread(
            () -> {
              try {
                executor.shutdown();
              } catch (Throwable t) {
                shutdownError.set(t);
              }
            });
    shutdownThread.setDaemon(true);
    shutdownThread.start();

    try {
      releasePool.countDown();
      long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ((executor.getPendingFuturesCount() > 0 || !executor.isIdle())
          && System.nanoTime() < deadlineNanos) {
        Thread.yield();
      }
      // shutdown() must still be waiting for the in-flight CallerRunsPolicy queue() call.
      assertThat(shutdownThread.isAlive()).isTrue();

      releaseCallerRuns.countDown();
      producer.join(5_000);
      shutdownThread.join(5_000);

      assertThat(producer.isAlive()).isFalse();
      assertThat(shutdownThread.isAlive()).isFalse();
      assertThat(producerError.get()).isNull();
      assertThat(shutdownError.get()).isInstanceOf(IOException.class);
      assertThat(shutdownError.get()).hasMessageThat().isEqualTo("in-flight caller-runs failure");
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);

      executor.shutdown();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
    } finally {
      releasePool.countDown();
      releaseCallerRuns.countDown();
    }
  }

  @Test
  void shutdown_interruptedWhileAwaitingInFlightQueueCall_cancelsLateFutureAndKeepsPendingZero()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch workerActive = new CountDownLatch(1);
    CountDownLatch releasePool = new CountDownLatch(1);
    CountDownLatch callerRunsEntered = new CountDownLatch(1);
    CountDownLatch releaseCallerRuns = new CountDownLatch(1);
    AtomicBoolean shutdownPreserveInterrupt = new AtomicBoolean(false);
    AtomicReference<Throwable> shutdownError = new AtomicReference<>();
    AtomicReference<Throwable> producerError = new AtomicReference<>();

    for (int i = 0; i < 21; i++) {
      final boolean isFirst = (i == 0);
      executor.queue(
          () -> {
            if (isFirst) {
              workerActive.countDown();
            }
            assertThat(releasePool.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
          },
          null);
    }
    assertThat(workerActive.await(5, TimeUnit.SECONDS)).isTrue();

    Thread producer =
        new Thread(
            () -> {
              try {
                executor.queue(
                    () -> {
                      callerRunsEntered.countDown();
                      assertThat(releaseCallerRuns.await(5, TimeUnit.SECONDS)).isTrue();
                      return "late-result";
                    },
                    null);
              } catch (Throwable t) {
                producerError.set(t);
              }
            });
    producer.setDaemon(true);
    producer.start();
    assertThat(callerRunsEntered.await(5, TimeUnit.SECONDS)).isTrue();

    Thread shutdownThread =
        new Thread(
            () -> {
              try {
                executor.shutdown();
              } catch (Throwable t) {
                shutdownPreserveInterrupt.set(Thread.currentThread().isInterrupted());
                shutdownError.set(t);
              } finally {
                Thread.interrupted();
              }
            });
    shutdownThread.setDaemon(true);
    shutdownThread.start();

    try {
      releasePool.countDown();
      long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ((executor.getPendingFuturesCount() > 0 || !executor.isIdle())
          && System.nanoTime() < deadlineNanos) {
        Thread.yield();
      }
      Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);

      shutdownThread.interrupt();
      shutdownThread.join(5_000);
      assertThat(shutdownThread.isAlive()).isFalse();
      assertThat(shutdownError.get()).isInstanceOf(InterruptedIOException.class);
      assertThat(shutdownPreserveInterrupt.get()).isTrue();

      // Finish the in-flight queue() call after shutdown() has already closed the executor.
      releaseCallerRuns.countDown();
      producer.join(5_000);
      assertThat(producer.isAlive()).isFalse();
      assertThat(producerError.get()).isNull();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);

      assertTimeoutPreemptively(Duration.ofMillis(500), executor::shutdown);
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
    } finally {
      releasePool.countDown();
      releaseCallerRuns.countDown();
    }
  }

  @Test
  void forceShutdownNow_withUnstartedQueuedFutures_cancelsDroppedTasks() throws Exception {
    ThreadPoolExecutor pool = BatchExecutor.newRequestExecutor(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    AtomicBoolean unstartedTaskRan = new AtomicBoolean(false);

    try {
      Future<Void> activeFuture =
          pool.submit(
              () -> {
                workerStarted.countDown();
                Uninterruptibles.awaitUninterruptibly(releaseWorker);
                return null;
              });
      assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<Integer> queuedFuture1 =
          pool.submit(
              () -> {
                unstartedTaskRan.set(true);
                return 1;
              });
      Future<Integer> queuedFuture2 =
          pool.submit(
              () -> {
                unstartedTaskRan.set(true);
                return 2;
              });

      BatchExecutor.forceShutdownNow(pool);

      assertThat(queuedFuture1.isDone()).isTrue();
      assertThat(queuedFuture1.isCancelled()).isTrue();
      assertThat(queuedFuture2.isDone()).isTrue();
      assertThat(queuedFuture2.isCancelled()).isTrue();
      assertThrows(CancellationException.class, () -> queuedFuture1.get(1, TimeUnit.SECONDS));
      assertThrows(CancellationException.class, () -> queuedFuture2.get(1, TimeUnit.SECONDS));
      assertThat(unstartedTaskRan.get()).isFalse();
      assertThat(activeFuture.isDone()).isFalse();
    } finally {
      releaseWorker.countDown();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void
      shutdown_whenAwaitTerminationTimesOutWithLateQueuedTask_cancelsDroppedTasksAndSubsequentShutdownReturnsImmediately()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWedgedWorker = new CountDownLatch(1);
    AtomicBoolean lateQueuedTaskRan = new AtomicBoolean(false);
    AtomicBoolean lateQueueAttempted = new AtomicBoolean(false);

    executor.queue(
        () -> {
          workerStarted.countDown();
          new CountDownLatch(1).await();
          return null;
        },
        new FutureCallback<Object>() {
          @Override
          public void onSuccess(@Nullable Object result) {}

          @Override
          public void onFailure(Throwable t) {
            lateQueueAttempted.set(true);
            try {
              executor.queue(
                  () -> {
                    lateQueuedTaskRan.set(true);
                    return null;
                  },
                  null);
            } catch (IllegalStateException ignored) {
              // Expected if shutdown gate closed before onFailure queued the follow-up task.
            }
            // Hold the worker past SHUTDOWN_GRACE_PERIOD_SECONDS so awaitTermination times out
            // and invokes forceShutdownNow().
            Uninterruptibles.awaitUninterruptibly(releaseWedgedWorker);
          }
        });

    assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
              callerThread.interrupt();
            });
    interrupter.setDaemon(true);
    interrupter.start();

    try {
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::shutdown);

      assertThat(Thread.interrupted()).isTrue();
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(lateQueueAttempted.get()).isTrue();
      assertThat(lateQueuedTaskRan.get()).isFalse();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);

      // Even while the wedged worker has not yet exited, idempotent shutdown() and
      // awaitRequestsCompletion() must return immediately without hanging.
      assertTimeoutPreemptively(Duration.ofMillis(500), executor::shutdown);
      assertTimeoutPreemptively(Duration.ofMillis(500), executor::awaitRequestsCompletion);
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
    } finally {
      releaseWedgedWorker.countDown();
      Thread.interrupted();
      interrupter.join(5_000);
    }
  }

  @Test
  void
      shutdown_whenInterruptedDuringAwaitTermination_cancelsTasksAndSubsequentShutdownReturnsImmediately()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch workerInCleanup = new CountDownLatch(1);
    CountDownLatch releaseWedgedWorker = new CountDownLatch(1);
    AtomicBoolean queuedSiblingRan = new AtomicBoolean(false);

    executor.queue(
        () -> {
          workerStarted.countDown();
          try {
            new CountDownLatch(1).await();
          } catch (InterruptedException e) {
            workerInCleanup.countDown();
            Uninterruptibles.awaitUninterruptibly(releaseWedgedWorker);
            throw e;
          }
          return null;
        },
        null);
    executor.queue(
        () -> {
          queuedSiblingRan.set(true);
          return null;
        },
        null);

    assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    Thread callerThread = Thread.currentThread();
    Thread interrupter =
        new Thread(
            () -> {
              Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
              callerThread.interrupt();
              if (Uninterruptibles.awaitUninterruptibly(workerInCleanup, 5, TimeUnit.SECONDS)) {
                // Interrupt a second time once caller is inside requestsExecutor.awaitTermination.
                Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
                callerThread.interrupt();
              }
            });
    interrupter.setDaemon(true);
    interrupter.start();

    try {
      InterruptedIOException exception =
          assertThrows(InterruptedIOException.class, executor::shutdown);

      assertThat(Thread.interrupted()).isTrue();
      assertThat(exception.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(queuedSiblingRan.get()).isFalse();
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);

      assertTimeoutPreemptively(Duration.ofMillis(500), executor::shutdown);
      assertThat(executor.getPendingFuturesCount()).isEqualTo(0);
    } finally {
      releaseWedgedWorker.countDown();
      Thread.interrupted();
      interrupter.join(5_000);
    }
  }

  @Test
  void shutdown_concurrentCallers_serializesExecutionAndNeverReturnsBeforeShutdownCompleted()
      throws Exception {
    BatchExecutor executor = new BatchExecutor(2);
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch releaseTask = new CountDownLatch(1);
    AtomicBoolean activeTaskWasInterrupted = new AtomicBoolean(false);
    AtomicBoolean activeTaskCompletedOrderly = new AtomicBoolean(false);
    AtomicInteger completionSequence = new AtomicInteger();
    AtomicInteger primaryFinishOrder = new AtomicInteger();
    AtomicInteger concurrentFinishOrder = new AtomicInteger();
    AtomicInteger interruptedConcurrentFinishOrder = new AtomicInteger();
    AtomicBoolean concurrentSawShutdownCompleted = new AtomicBoolean(false);
    AtomicBoolean interruptedConcurrentSawShutdownCompleted = new AtomicBoolean(false);
    AtomicBoolean interruptedConcurrentPreservedInterrupt = new AtomicBoolean(false);
    AtomicReference<Throwable> primaryError = new AtomicReference<>();
    AtomicReference<Throwable> concurrentError = new AtomicReference<>();
    AtomicReference<Throwable> interruptedConcurrentError = new AtomicReference<>();

    executor.queue(
        () -> {
          taskStarted.countDown();
          try {
            assertThat(releaseTask.await(5, TimeUnit.SECONDS)).isTrue();
            activeTaskCompletedOrderly.set(true);
          } catch (InterruptedException e) {
            activeTaskWasInterrupted.set(true);
            throw e;
          }
          return null;
        },
        null);
    assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();

    Thread primaryShutdown =
        new Thread(
            () -> {
              try {
                executor.shutdown();
              } catch (Throwable t) {
                primaryError.set(t);
              } finally {
                primaryFinishOrder.set(completionSequence.incrementAndGet());
              }
            });
    primaryShutdown.setDaemon(true);
    primaryShutdown.start();

    long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (primaryShutdown.getState() != Thread.State.WAITING
        && primaryShutdown.getState() != Thread.State.TIMED_WAITING
        && System.nanoTime() < waitDeadline) {
      Thread.yield();
    }
    assertThat(executor.isShutdownCompleted()).isFalse();

    Thread concurrentShutdown =
        new Thread(
            () -> {
              try {
                executor.shutdown();
                concurrentSawShutdownCompleted.set(executor.isShutdownCompleted());
              } catch (Throwable t) {
                concurrentError.set(t);
              } finally {
                concurrentFinishOrder.set(completionSequence.incrementAndGet());
              }
            });
    Thread interruptedConcurrentShutdown =
        new Thread(
            () -> {
              try {
                executor.shutdown();
                interruptedConcurrentSawShutdownCompleted.set(executor.isShutdownCompleted());
              } catch (Throwable t) {
                interruptedConcurrentError.set(t);
              } finally {
                interruptedConcurrentPreservedInterrupt.set(Thread.currentThread().isInterrupted());
                Thread.interrupted();
                interruptedConcurrentFinishOrder.set(completionSequence.incrementAndGet());
              }
            });
    concurrentShutdown.setDaemon(true);
    interruptedConcurrentShutdown.setDaemon(true);
    concurrentShutdown.start();
    interruptedConcurrentShutdown.start();

    long monitorDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while ((concurrentShutdown.getState() != Thread.State.BLOCKED
            || interruptedConcurrentShutdown.getState() != Thread.State.BLOCKED)
        && System.nanoTime() < monitorDeadline) {
      Thread.yield();
    }

    // Interrupting a queued concurrent shutdown() caller must not cancel the primary drain or allow
    // either concurrent caller to return before primaryShutdown completes.
    interruptedConcurrentShutdown.interrupt();
    Uninterruptibles.sleepUninterruptibly(25, TimeUnit.MILLISECONDS);
    assertThat(activeTaskWasInterrupted.get()).isFalse();
    assertThat(concurrentShutdown.isAlive()).isTrue();
    assertThat(interruptedConcurrentShutdown.isAlive()).isTrue();
    assertThat(executor.isShutdownCompleted()).isFalse();

    releaseTask.countDown();
    primaryShutdown.join(5_000);
    concurrentShutdown.join(5_000);
    interruptedConcurrentShutdown.join(5_000);

    assertThat(primaryShutdown.isAlive()).isFalse();
    assertThat(concurrentShutdown.isAlive()).isFalse();
    assertThat(interruptedConcurrentShutdown.isAlive()).isFalse();
    assertThat(primaryError.get()).isNull();
    assertThat(concurrentError.get()).isNull();
    assertThat(interruptedConcurrentError.get()).isNull();
    assertThat(activeTaskCompletedOrderly.get()).isTrue();
    assertThat(activeTaskWasInterrupted.get()).isFalse();
    assertThat(concurrentSawShutdownCompleted.get()).isTrue();
    assertThat(interruptedConcurrentSawShutdownCompleted.get()).isTrue();
    assertThat(interruptedConcurrentPreservedInterrupt.get()).isTrue();
    assertThat(primaryFinishOrder.get()).isEqualTo(1);
    assertThat(concurrentFinishOrder.get()).isGreaterThan(1);
    assertThat(interruptedConcurrentFinishOrder.get()).isGreaterThan(1);
    assertThat(executor.isShutdownCompleted()).isTrue();
  }

  @Test
  void
      awaitRequestsCompletion_whileConcurrentQueueHarvestsDoneFailure_waitsForHarvestAndThrowsFailure()
          throws Exception {
    BatchExecutor executor = new BatchExecutor(0);
    CountDownLatch harvestEntered = new CountDownLatch(1);
    CountDownLatch releaseHarvest = new CountDownLatch(1);
    AtomicReference<Throwable> producerError = new AtomicReference<>();
    AtomicReference<Throwable> awaitError = new AtomicReference<>();

    InterruptedIOException customHarvestFailure =
        new InterruptedIOException("in-flight harvested failure") {
          @Override
          public synchronized Throwable getCause() {
            harvestEntered.countDown();
            Uninterruptibles.awaitUninterruptibly(releaseHarvest);
            return null;
          }
        };

    Thread producer =
        new Thread(
            () -> {
              try {
                executor.queue(
                    () -> {
                      throw customHarvestFailure;
                    },
                    null);
              } catch (Throwable t) {
                producerError.set(t);
              }
            });
    producer.setDaemon(true);
    producer.start();
    assertThat(harvestEntered.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executor.getPendingFuturesCount()).isEqualTo(0);

    Thread awaiter =
        new Thread(
            () -> {
              try {
                executor.awaitRequestsCompletion();
              } catch (Throwable t) {
                awaitError.set(t);
              }
            });
    awaiter.setDaemon(true);
    awaiter.start();

    try {
      long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (awaiter.getState() != Thread.State.WAITING && System.nanoTime() < waitDeadline) {
        Thread.yield();
      }
      // awaitRequestsCompletion() must block while queue() is still harvesting popped doneFutures.
      assertThat(awaiter.isAlive()).isTrue();

      releaseHarvest.countDown();
      producer.join(5_000);
      awaiter.join(5_000);

      assertThat(producer.isAlive()).isFalse();
      assertThat(awaiter.isAlive()).isFalse();
      assertThat(producerError.get()).isNull();
      assertThat(awaitError.get()).isSameInstanceAs(customHarvestFailure);
    } finally {
      releaseHarvest.countDown();
      executor.shutdown();
    }
  }

  @Test
  void
      queue_withPreInterruptedCallerAndInterruptClearingCallerRunTask_shieldsTaskAndPreservesInterrupt()
          throws Exception {
    BatchExecutor directExecutor = new BatchExecutor(0);
    AtomicBoolean interruptedInsideDirectTask = new AtomicBoolean(true);
    AtomicInteger directCompleted = new AtomicInteger();

    Thread.currentThread().interrupt();
    try {
      directExecutor.queue(
          () -> {
            interruptedInsideDirectTask.set(Thread.currentThread().isInterrupted());
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            Thread.interrupted();
            return directCompleted.incrementAndGet();
          },
          null);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      directExecutor.queue(
          () -> {
            try {
              throw new InterruptedException("swallowed internal interruption");
            } catch (InterruptedException ignored) {
              return directCompleted.incrementAndGet();
            }
          },
          null);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();

      directExecutor.awaitRequestsCompletion();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      directExecutor.shutdown();

      assertThat(interruptedInsideDirectTask.get()).isFalse();
      assertThat(directCompleted.get()).isEqualTo(2);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      directExecutor.shutdown();
    }

    // Also verify CallerRunsPolicy saturation when caller thread is pre-interrupted.
    BatchExecutor pooledExecutor = new BatchExecutor(1);
    CountDownLatch workerStarted = new CountDownLatch(1);
    CountDownLatch releaseWorker = new CountDownLatch(1);
    AtomicReference<Thread> callerRunsThread = new AtomicReference<>();
    AtomicBoolean interruptedInsideCallerRunsTask = new AtomicBoolean(true);
    AtomicBoolean callerRunsTaskCompleted = new AtomicBoolean(false);

    try {
      for (int i = 0; i < 21; i++) {
        final boolean isFirst = (i == 0);
        pooledExecutor.queue(
            () -> {
              if (isFirst) {
                workerStarted.countDown();
              }
              assertThat(releaseWorker.await(5, TimeUnit.SECONDS)).isTrue();
              return null;
            },
            null);
      }
      assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Thread.currentThread().interrupt();
      pooledExecutor.queue(
          () -> {
            callerRunsThread.set(Thread.currentThread());
            interruptedInsideCallerRunsTask.set(Thread.currentThread().isInterrupted());
            assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            Thread.interrupted();
            callerRunsTaskCompleted.set(true);
            return null;
          },
          null);

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(callerRunsThread.get()).isEqualTo(Thread.currentThread());
      assertThat(interruptedInsideCallerRunsTask.get()).isFalse();
      assertThat(callerRunsTaskCompleted.get()).isTrue();
    } finally {
      Thread.interrupted();
      releaseWorker.countDown();
      pooledExecutor.shutdown();
    }
  }

  private static <T> FutureCallback<T> recordingListCallback(
      Collection<T> successes, Collection<Throwable> failures) {
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(@Nullable T result) {
        successes.add(result);
      }

      @Override
      public void onFailure(Throwable throwable) {
        failures.add(throwable);
      }
    };
  }

  private static <T> FutureCallback<T> recordCallback(
      @Nullable Collection<T> successes, @Nullable SettableFuture<Throwable> failure) {
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(@Nullable T result) {
        if (successes != null) {
          successes.add(result);
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (failure != null) {
          failure.set(throwable);
        }
      }
    };
  }
}
