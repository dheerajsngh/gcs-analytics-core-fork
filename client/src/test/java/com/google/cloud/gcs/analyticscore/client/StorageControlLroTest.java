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

import com.google.api.core.ApiClock;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationFutureImpl;
import com.google.api.gax.longrunning.OperationResponsePollAlgorithm;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.retrying.DirectRetryingExecutor;
import com.google.api.gax.retrying.RetryAlgorithm;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.api.gax.retrying.ScheduledRetryingExecutor;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.api.gax.retrying.TimedRetryAlgorithm;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.CancelledException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnauthenticatedException;
import com.google.storage.control.v2.CommonLongRunningOperationMetadata;
import com.google.storage.control.v2.DeleteFolderRecursiveMetadata;
import com.google.storage.control.v2.Folder;
import com.google.storage.control.v2.RenameFolderMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class StorageControlLroTest {

  @AfterEach
  void clearThreadInterrupt() {
    Thread.interrupted();
  }

  @Test
  void defaultConstants_matchSpecification() {
    assertThat(StorageControlLro.DEFAULT_MIN_POLL_DELAY).isEqualTo(Duration.ofMillis(50));
    assertThat(StorageControlLro.DEFAULT_MAX_POLL_DELAY).isEqualTo(Duration.ofSeconds(30));
    assertThat(StorageControlLro.DEFAULT_POLL_PROPORTION).isEqualTo(0.05);
    assertThat(StorageControlLro.DEFAULT_TOTAL_TIMEOUT).isEqualTo(Duration.ofMinutes(15));
  }

  @Test
  void constructor_isPrivate() throws Exception {
    Constructor<StorageControlLro> constructor = StorageControlLro.class.getDeclaredConstructor();

    assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue();
  }

  @Test
  void createLinearProportionalPollAlgorithm_invalidArguments_throwsException() {
    FakeApiClock clock = new FakeApiClock(0L);
    Duration min = Duration.ofMillis(50);
    Duration max = Duration.ofSeconds(30);
    Duration timeout = Duration.ofMinutes(15);

    assertThrows(
        NullPointerException.class,
        () -> StorageControlLro.createLinearProportionalPollAlgorithm(null, max, 0.05, timeout));
    assertThrows(
        NullPointerException.class,
        () -> StorageControlLro.createLinearProportionalPollAlgorithm(min, null, 0.05, timeout));
    assertThrows(
        NullPointerException.class,
        () -> StorageControlLro.createLinearProportionalPollAlgorithm(min, max, 0.05, null));
    assertThrows(
        NullPointerException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(min, max, 0.05, timeout, null));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                Duration.ZERO, max, 0.05, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                Duration.ofMillis(-1), max, 0.05, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                Duration.ofSeconds(2), Duration.ofSeconds(1), 0.05, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(min, max, 0.0, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                min, max, -0.05, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                min, max, Double.NaN, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                min, max, Double.POSITIVE_INFINITY, timeout, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                min, max, 0.05, Duration.ZERO, clock));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StorageControlLro.createLinearProportionalPollAlgorithm(
                min, max, 0.05, Duration.ofSeconds(-5), clock));
  }

  @Test
  void pollAlgorithm_followsFivePercentLinearProgressionClampedAndCappedToRemainingBudget() {
    FakeApiClock clock = new FakeApiClock(1_000_000_000L);
    TimedRetryAlgorithm algorithm =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            StorageControlLro.DEFAULT_MIN_POLL_DELAY,
            StorageControlLro.DEFAULT_MAX_POLL_DELAY,
            StorageControlLro.DEFAULT_POLL_PROPORTION,
            StorageControlLro.DEFAULT_TOTAL_TIMEOUT,
            clock);

    TimedAttemptSettings first = algorithm.createFirstAttempt();
    assertThat(first.getRetryDelayDuration()).isEqualTo(Duration.ZERO);
    assertThat(first.getRandomizedRetryDelayDuration()).isEqualTo(Duration.ZERO);
    assertThat(first.getAttemptCount()).isEqualTo(0);
    assertThat(algorithm.shouldRetry(first)).isTrue();

    // 100ms elapsed -> 5% is 5ms, clamped up to 50ms minDelay.
    clock.setNanos(1_000_000_000L + Duration.ofMillis(100).toNanos());
    TimedAttemptSettings attempt1 = algorithm.createNextAttempt(first);
    assertThat(attempt1.getRetryDelayDuration()).isEqualTo(Duration.ofMillis(50));
    assertThat(attempt1.getRandomizedRetryDelayDuration()).isEqualTo(Duration.ofMillis(50));
    assertThat(attempt1.getAttemptCount()).isEqualTo(1);
    assertThat(algorithm.shouldRetry(attempt1)).isTrue();

    // 10s elapsed -> 5% is 500ms (linear region).
    clock.setNanos(1_000_000_000L + Duration.ofSeconds(10).toNanos());
    TimedAttemptSettings attempt2 = algorithm.createNextAttempt(attempt1);
    assertThat(attempt2.getRetryDelayDuration()).isEqualTo(Duration.ofMillis(500));
    assertThat(attempt2.getRandomizedRetryDelayDuration()).isEqualTo(Duration.ofMillis(500));
    assertThat(algorithm.shouldRetry(attempt2)).isTrue();

    // 100s elapsed -> 5% is 5s (linear region).
    clock.setNanos(1_000_000_000L + Duration.ofSeconds(100).toNanos());
    TimedAttemptSettings attempt3 = algorithm.createNextAttempt(attempt2);
    assertThat(attempt3.getRetryDelayDuration()).isEqualTo(Duration.ofSeconds(5));

    // 700s elapsed -> 5% is 35s, clamped down to 30s maxDelay.
    clock.setNanos(1_000_000_000L + Duration.ofSeconds(700).toNanos());
    TimedAttemptSettings attempt4 = algorithm.createNextAttempt(attempt3);
    assertThat(attempt4.getRetryDelayDuration()).isEqualTo(Duration.ofSeconds(30));
    assertThat(algorithm.shouldRetry(attempt4)).isTrue();

    // 890s elapsed (10s remaining before 900s = 15m deadline) -> capped to 10s remaining budget.
    clock.setNanos(1_000_000_000L + Duration.ofSeconds(890).toNanos());
    TimedAttemptSettings attempt5 = algorithm.createNextAttempt(attempt4);
    assertThat(attempt5.getRetryDelayDuration()).isEqualTo(Duration.ofSeconds(10));
    assertThat(algorithm.shouldRetry(attempt5)).isTrue();

    // 899.98s elapsed (20ms remaining, below 50ms minDelay) -> capped to 20ms remaining budget.
    clock.setNanos(1_000_000_000L + Duration.ofMillis(899_980).toNanos());
    TimedAttemptSettings attempt6 = algorithm.createNextAttempt(attempt5);
    assertThat(attempt6.getRetryDelayDuration()).isEqualTo(Duration.ofMillis(20));
    assertThat(algorithm.shouldRetry(attempt6)).isTrue();
  }

  @Test
  void shouldRetry_atAndPastTotalTimeout_throwsCancellationExceptionNeverReturnsFalse() {
    FakeApiClock clock = new FakeApiClock(500L);
    Duration totalTimeout = Duration.ofMinutes(15);
    TimedRetryAlgorithm algorithm =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            Duration.ofMillis(50), Duration.ofSeconds(30), 0.05, totalTimeout, clock);

    TimedAttemptSettings first = algorithm.createFirstAttempt();
    clock.advance(totalTimeout.minusNanos(1));
    assertThat(algorithm.shouldRetry(first)).isTrue();

    // Exact deadline expiration.
    clock.advance(Duration.ofNanos(1));
    TimedAttemptSettings expiredAttempt = algorithm.createNextAttempt(first);
    assertThat(expiredAttempt.getRetryDelayDuration()).isEqualTo(Duration.ZERO);
    CancellationException exactTimeout =
        assertThrows(CancellationException.class, () -> algorithm.shouldRetry(expiredAttempt));
    assertThat(exactTimeout)
        .hasMessageThat()
        .isEqualTo("LRO polling timed out after " + totalTimeout);

    // Past deadline expiration.
    clock.advance(Duration.ofSeconds(5));
    assertThrows(CancellationException.class, () -> algorithm.shouldRetry(expiredAttempt));
  }

  @Test
  void pollAlgorithm_sharedAcrossConcurrentOperations_maintainsIndependentDeadlines() {
    FakeApiClock clock = new FakeApiClock(0L);
    Duration totalTimeout = Duration.ofSeconds(100);
    TimedRetryAlgorithm sharedAlgorithm =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            Duration.ofMillis(50), Duration.ofSeconds(30), 0.05, totalTimeout, clock);

    TimedAttemptSettings op1First = sharedAlgorithm.createFirstAttempt();
    clock.advance(Duration.ofSeconds(40));
    TimedAttemptSettings op2First = sharedAlgorithm.createFirstAttempt();

    // At t = 100s: op1 has reached its 100s deadline, whereas op2 has only elapsed 60s (5% = 3s).
    clock.advance(Duration.ofSeconds(60));
    TimedAttemptSettings op1Next = sharedAlgorithm.createNextAttempt(op1First);
    TimedAttemptSettings op2Next = sharedAlgorithm.createNextAttempt(op2First);

    assertThrows(CancellationException.class, () -> sharedAlgorithm.shouldRetry(op1Next));
    assertThat(sharedAlgorithm.shouldRetry(op2Next)).isTrue();
    assertThat(op2Next.getRetryDelayDuration()).isEqualTo(Duration.ofSeconds(3));
  }

  @Test
  void pollAlgorithm_withRealGaxOperationFuture_completesOnDoneAndCancelsOnTimeout()
      throws Exception {
    FakeApiClock clock = new FakeApiClock(0L);
    List<Duration> recordedSleeps = new ArrayList<>();
    TimedRetryAlgorithm pollAlgorithm =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            Duration.ofMillis(50), Duration.ofSeconds(30), 0.05, Duration.ofSeconds(10), clock);
    RetryAlgorithm<OperationSnapshot> retryAlgorithm =
        new RetryAlgorithm<>(new OperationResponsePollAlgorithm(), pollAlgorithm);
    DirectRetryingExecutor<OperationSnapshot> executor =
        new DirectRetryingExecutor<>(retryAlgorithm) {
          @Override
          protected void sleep(Duration delay) {
            recordedSleeps.add(delay);
            clock.advance(delay);
          }
        };

    // Case 1: Completes after 3 polls; verify GAX OperationFutureImpl.peekMetadata() null safety
    // and linear delay progression.
    AtomicInteger pollCalls = new AtomicInteger();
    Folder expectedFolder = Folder.newBuilder().setName("buckets/b/folders/f/").build();
    OperationSnapshot initialSnapshot = new FakeOperationSnapshot("operations/rename-1", false);
    RetryingFuture<OperationSnapshot> pollingFuture =
        executor.createFuture(
            () -> {
              int call = pollCalls.incrementAndGet();
              return new FakeOperationSnapshot("operations/rename-1", call >= 3);
            });
    OperationFutureImpl<Folder, RenameFolderMetadata> successOpFuture =
        new OperationFutureImpl<>(
            pollingFuture,
            ApiFutures.immediateFuture(initialSnapshot),
            snapshot -> {
              assertThat(snapshot.isDone()).isTrue();
              return expectedFolder;
            },
            snapshot -> RenameFolderMetadata.getDefaultInstance());
    // Before submit, real GAX OperationFutureImpl.peekMetadata() returns null!
    assertThat(successOpFuture.peekMetadata()).isNull();
    assertThat(StorageControlLro.safePeekMetadata(successOpFuture)).isEmpty();

    pollingFuture.setAttemptFuture(executor.submit(pollingFuture));
    Folder actual = StorageControlLro.awaitOperation(successOpFuture, "Rename folder");
    assertThat(actual).isEqualTo(expectedFolder);
    assertThat(recordedSleeps)
        .containsExactly(Duration.ZERO, Duration.ofMillis(50), Duration.ofMillis(50))
        .inOrder();

    // Case 2: Never completes before 10s totalTimeout -> shouldRetry throws CancellationException,
    // GAX cancels the polling future rather than returning a non-done snapshot, and awaitOperation
    // translates it into a timeout IOException.
    RetryingFuture<OperationSnapshot> neverDonePolling =
        executor.createFuture(() -> new FakeOperationSnapshot("operations/rename-timeout", false));
    OperationFutureImpl<Folder, RenameFolderMetadata> timeoutOpFuture =
        new OperationFutureImpl<>(
            neverDonePolling,
            ApiFutures.immediateFuture(
                new FakeOperationSnapshot("operations/rename-timeout", false)),
            snapshot -> {
              throw new AssertionError(
                  "Response transformer must never run on non-done snapshot: " + snapshot.isDone());
            },
            snapshot -> RenameFolderMetadata.getDefaultInstance());
    neverDonePolling.setAttemptFuture(executor.submit(neverDonePolling));

    IOException timeoutIo =
        assertThrows(
            IOException.class,
            () -> StorageControlLro.awaitOperation(timeoutOpFuture, "Rename folder"));
    assertThat(timeoutIo).hasMessageThat().contains("Rename folder timed out");
    assertThat(timeoutIo).hasMessageThat().contains("operations/rename-timeout");
    assertThat(timeoutIo).hasCauseThat().isInstanceOf(CancellationException.class);
  }

  @Test
  void
      awaitOperation_onInterruptedExceptionWithIncompleteInitialFuture_cancelsBeforeInterruptWithoutBlockingOnGetName() {
    SettableApiFuture<OperationSnapshot> incompleteInitialFuture = SettableApiFuture.create();
    AtomicBoolean cancelCalled = new AtomicBoolean(false);
    AtomicBoolean interruptedDuringCancel = new AtomicBoolean(true);
    InterruptedException rootInterrupt = new InterruptedException("caller interrupted");

    FakeOperationFuture<Folder, RenameFolderMetadata> opFuture =
        new FakeOperationFuture<Folder, RenameFolderMetadata>(incompleteInitialFuture, null) {
          @Override
          public Folder get() throws InterruptedException {
            // Simulate Future.get() clearing interrupt status when throwing InterruptedException.
            Thread.interrupted();
            throw rootInterrupt;
          }

          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            assertThat(mayInterruptIfRunning).isTrue();
            interruptedDuringCancel.set(Thread.currentThread().isInterrupted());
            cancelCalled.set(true);
            return super.cancel(mayInterruptIfRunning);
          }

          @Override
          public String getName() {
            throw new AssertionError("getName() must never be called because it blocks!");
          }
        };

    InterruptedIOException thrown =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(opFuture, "Delete folder gs://b/dir/"));

    assertThat(cancelCalled.get()).isTrue();
    assertThat(interruptedDuringCancel.get()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(thrown).hasMessageThat().contains("Delete folder gs://b/dir/ was interrupted");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(rootInterrupt);
  }

  @Test
  void awaitOperation_onInterruptedExceptionWithCompletedInitialFuture_includesSafeOperationName() {
    ApiFuture<OperationSnapshot> completedInitial =
        ApiFutures.immediateFuture(new FakeOperationSnapshot("operations/del-42", false));
    DeleteFolderRecursiveMetadata metadata =
        DeleteFolderRecursiveMetadata.newBuilder()
            .setCommonMetadata(
                CommonLongRunningOperationMetadata.newBuilder().setProgressPercent(75).build())
            .build();
    InterruptedException rootInterrupt = new InterruptedException("stop");

    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> opFuture =
        new FakeOperationFuture<Void, DeleteFolderRecursiveMetadata>(
            completedInitial, ApiFutures.immediateFuture(metadata)) {
          @Override
          public Void get() throws InterruptedException {
            Thread.interrupted();
            throw rootInterrupt;
          }
        };

    InterruptedIOException thrown =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(opFuture, "Recursive delete"));

    assertThat(opFuture.wasCancelledWithInterrupt()).isTrue();
    assertThat(opFuture.peekMetadata().isCancelled()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(thrown).hasMessageThat().contains("Recursive delete was interrupted");
    assertThat(thrown).hasMessageThat().contains("operations/del-42");
    assertThat(thrown).hasMessageThat().contains("progress=75%");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(rootInterrupt);
  }

  @Test
  void
      awaitOperation_withRealScheduledGaxOperationFuture_preservesProgressBeforeCancelInvalidatesMetadata()
          throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    try {
      DeleteFolderRecursiveMetadata progress75 =
          DeleteFolderRecursiveMetadata.newBuilder()
              .setCommonMetadata(
                  CommonLongRunningOperationMetadata.newBuilder().setProgressPercent(75).build())
              .build();
      TimedRetryAlgorithm pollAlgorithm =
          StorageControlLro.createLinearProportionalPollAlgorithm(
              Duration.ofSeconds(10), Duration.ofSeconds(30), 0.05, Duration.ofMinutes(15));
      RetryAlgorithm<OperationSnapshot> retryAlgorithm =
          new RetryAlgorithm<>(new OperationResponsePollAlgorithm(), pollAlgorithm);
      ScheduledRetryingExecutor<OperationSnapshot> executor =
          new ScheduledRetryingExecutor<>(retryAlgorithm, scheduler);

      // Case 1: Explicit Duration timeout mid-poll preserves progress=75% before cancel(true)
      // overwrites GAX latestCompletedAttemptResult with a cancelled future.
      RetryingFuture<OperationSnapshot> timeoutPolling =
          executor.createFuture(
              () -> new FakeOperationSnapshot("operations/del-sched-timeout", false, progress75));
      OperationFutureImpl<Void, DeleteFolderRecursiveMetadata> timeoutOpFuture =
          new OperationFutureImpl<>(
              timeoutPolling,
              ApiFutures.immediateFuture(
                  new FakeOperationSnapshot("operations/del-sched-timeout", false, progress75)),
              snapshot -> null,
              snapshot -> (DeleteFolderRecursiveMetadata) snapshot.getMetadata());
      ApiFuture<DeleteFolderRecursiveMetadata> firstTimeoutPollMeta = timeoutOpFuture.getMetadata();
      timeoutPolling.setAttemptFuture(executor.submit(timeoutPolling));
      assertThat(firstTimeoutPollMeta.get(5, TimeUnit.SECONDS)).isEqualTo(progress75);
      assertThat(timeoutOpFuture.peekMetadata().isCancelled()).isFalse();

      IOException timeoutThrown =
          assertThrows(
              IOException.class,
              () ->
                  StorageControlLro.awaitOperation(
                      timeoutOpFuture, "Recursive delete", Duration.ofMillis(10)));
      assertThat(timeoutThrown).hasCauseThat().isInstanceOf(TimeoutException.class);
      assertThat(timeoutThrown).hasMessageThat().contains("Recursive delete timed out");
      assertThat(timeoutThrown).hasMessageThat().contains("operations/del-sched-timeout");
      assertThat(timeoutThrown).hasMessageThat().contains("progress=75%");
      assertThat(timeoutOpFuture.isCancelled()).isTrue();
      assertThat(timeoutOpFuture.peekMetadata().isCancelled()).isTrue();

      // Case 2: Mid-poll interruption preserves progress=75% before cancel(true) clobbers
      // peekMetadata(), and restores the caller interrupt flag afterward.
      RetryingFuture<OperationSnapshot> interruptPolling =
          executor.createFuture(
              () -> new FakeOperationSnapshot("operations/del-sched-interrupt", false, progress75));
      OperationFutureImpl<Void, DeleteFolderRecursiveMetadata> interruptOpFuture =
          new OperationFutureImpl<>(
              interruptPolling,
              ApiFutures.immediateFuture(
                  new FakeOperationSnapshot("operations/del-sched-interrupt", false, progress75)),
              snapshot -> null,
              snapshot -> (DeleteFolderRecursiveMetadata) snapshot.getMetadata());
      ApiFuture<DeleteFolderRecursiveMetadata> firstInterruptPollMeta =
          interruptOpFuture.getMetadata();
      interruptPolling.setAttemptFuture(executor.submit(interruptPolling));
      assertThat(firstInterruptPollMeta.get(5, TimeUnit.SECONDS)).isEqualTo(progress75);

      Thread callerThread = Thread.currentThread();
      Future<?> unusedInterruptTask =
          scheduler.schedule(callerThread::interrupt, 15, TimeUnit.MILLISECONDS);
      assertThat(unusedInterruptTask).isNotNull();

      InterruptedIOException interruptThrown =
          assertThrows(
              InterruptedIOException.class,
              () -> StorageControlLro.awaitOperation(interruptOpFuture, "Recursive delete"));
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(interruptThrown).hasMessageThat().contains("Recursive delete was interrupted");
      assertThat(interruptThrown).hasMessageThat().contains("operations/del-sched-interrupt");
      assertThat(interruptThrown).hasMessageThat().contains("progress=75%");
      assertThat(interruptOpFuture.isCancelled()).isTrue();
      assertThat(interruptOpFuture.peekMetadata().isCancelled()).isTrue();
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  void awaitOperation_withExplicitTimeout_passesMillisAndTranslatesTimeoutException() {
    TimeoutException timeoutCause = new TimeoutException("deadline");
    AtomicLong capturedTimeoutMillis = new AtomicLong(-1L);

    FakeOperationFuture<Folder, RenameFolderMetadata> opFuture =
        new FakeOperationFuture<Folder, RenameFolderMetadata>(
            ApiFutures.immediateFuture(new FakeOperationSnapshot("operations/ren-7", false)),
            null) {
          @Override
          public Folder get(long timeout, TimeUnit unit) throws TimeoutException {
            assertThat(unit).isEqualTo(TimeUnit.MILLISECONDS);
            capturedTimeoutMillis.set(timeout);
            throw timeoutCause;
          }
        };

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                StorageControlLro.awaitOperation(opFuture, "Rename folder", Duration.ofSeconds(5)));

    assertThat(capturedTimeoutMillis.get()).isEqualTo(5000L);
    assertThat(opFuture.wasCancelledWithInterrupt()).isTrue();
    assertThat(thrown).hasMessageThat().contains("Rename folder timed out");
    assertThat(thrown).hasMessageThat().contains("operations/ren-7");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(timeoutCause);
  }

  @Test
  void awaitOperation_directCancellationAndRenameMetadataProgress_cancelsAndFormatsProgress() {
    CancellationException directCancel = new CancellationException("cancelled");
    RenameFolderMetadata renameMetadata =
        RenameFolderMetadata.newBuilder()
            .setCommonMetadata(
                CommonLongRunningOperationMetadata.newBuilder().setProgressPercent(40).build())
            .build();
    FakeOperationFuture<Folder, RenameFolderMetadata> opFuture =
        new FakeOperationFuture<Folder, RenameFolderMetadata>(
            ApiFutures.immediateFuture(new FakeOperationSnapshot("operations/ren-40", false)),
            ApiFutures.immediateFuture(renameMetadata)) {
          @Override
          public Folder get() {
            throw directCancel;
          }
        };

    IOException thrown =
        assertThrows(
            IOException.class, () -> StorageControlLro.awaitOperation(opFuture, "Rename folder"));

    assertThat(opFuture.wasCancelledWithInterrupt()).isTrue();
    assertThat(thrown).hasMessageThat().contains("Rename folder timed out");
    assertThat(thrown).hasMessageThat().contains("operations/ren-40");
    assertThat(thrown).hasMessageThat().contains("progress=40%");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(directCancel);
    assertThrows(
        IllegalArgumentException.class,
        () -> StorageControlLro.awaitOperation(opFuture, "Rename folder", Duration.ofMillis(-1)));
  }

  @Test
  void awaitOperation_executionExceptionWrappingCancellationException_throwsTimeoutIoException() {
    CancellationException cancelCause = new CancellationException("LRO polling timed out");
    DeleteFolderRecursiveMetadata metadata =
        DeleteFolderRecursiveMetadata.newBuilder()
            .setCommonMetadata(
                CommonLongRunningOperationMetadata.newBuilder().setProgressPercent(55).build())
            .build();
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> opFuture =
        FakeOperationFuture.failingWith(cancelCause, "operations/del-cancel", metadata);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> StorageControlLro.awaitOperation(opFuture, "Delete folder recursively"));

    assertThat(opFuture.wasCancelledWithInterrupt()).isTrue();
    assertThat(opFuture.peekMetadata().isCancelled()).isTrue();
    assertThat(thrown).hasMessageThat().contains("Delete folder recursively timed out");
    assertThat(thrown).hasMessageThat().contains("operations/del-cancel");
    assertThat(thrown).hasMessageThat().contains("progress=55%");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(cancelCause);
  }

  @Test
  void awaitOperation_executionExceptionWrappingInterruptedException_cancelsAndRestoresInterrupt() {
    InterruptedException interruptCause = new InterruptedException("worker interrupted");
    AtomicBoolean interruptedDuringCancel = new AtomicBoolean(true);
    DeleteFolderRecursiveMetadata metadata =
        DeleteFolderRecursiveMetadata.newBuilder()
            .setCommonMetadata(
                CommonLongRunningOperationMetadata.newBuilder().setProgressPercent(60).build())
            .build();
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> opFuture =
        new FakeOperationFuture<Void, DeleteFolderRecursiveMetadata>(
            ApiFutures.immediateFuture(new FakeOperationSnapshot("operations/del-int", false)),
            ApiFutures.immediateFuture(metadata)) {
          @Override
          public Void get() throws ExecutionException {
            throw new ExecutionException(interruptCause);
          }

          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            interruptedDuringCancel.set(Thread.currentThread().isInterrupted());
            return super.cancel(mayInterruptIfRunning);
          }
        };

    InterruptedIOException thrown =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(opFuture, "Delete folder"));

    assertThat(opFuture.wasCancelledWithInterrupt()).isTrue();
    assertThat(opFuture.peekMetadata().isCancelled()).isTrue();
    assertThat(interruptedDuringCancel.get()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(thrown).hasMessageThat().contains("operations/del-int");
    assertThat(thrown).hasMessageThat().contains("progress=60%");
    assertThat(thrown).hasCauseThat().isSameInstanceAs(interruptCause);
  }

  @Test
  void awaitOperation_executionExceptionWrappingError_rethrowsErrorDirectlyWithoutWrapping() {
    AssertionError expectedError = new AssertionError("invariant violated");
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> opFuture =
        FakeOperationFuture.failingWith(expectedError, "operations/op-error");

    AssertionError rethrown =
        assertThrows(
            AssertionError.class,
            () -> StorageControlLro.awaitOperation(opFuture, "Delete folder"));
    assertThat(rethrown).isSameInstanceAs(expectedError);

    // Also verify real GAX OperationFutureImpl when responseTransformer throws an Error.
    RetryAlgorithm<OperationSnapshot> retryAlgorithm =
        new RetryAlgorithm<>(
            new OperationResponsePollAlgorithm(), StorageControlLro.createDefaultPollAlgorithm());
    DirectRetryingExecutor<OperationSnapshot> executor =
        new DirectRetryingExecutor<>(retryAlgorithm);
    RetryingFuture<OperationSnapshot> pollingFuture =
        executor.createFuture(() -> new FakeOperationSnapshot("operations/op-gax-err", true));
    AssertionError transformerError = new AssertionError("transformer failure");
    OperationFutureImpl<Folder, RenameFolderMetadata> gaxOpFuture =
        new OperationFutureImpl<>(
            pollingFuture,
            ApiFutures.immediateFuture(new FakeOperationSnapshot("operations/op-gax-err", true)),
            snapshot -> {
              throw transformerError;
            },
            snapshot -> RenameFolderMetadata.getDefaultInstance());
    pollingFuture.setAttemptFuture(executor.submit(pollingFuture));

    AssertionError gaxRethrown =
        assertThrows(
            AssertionError.class,
            () -> StorageControlLro.awaitOperation(gaxOpFuture, "Rename folder"));
    assertThat(gaxRethrown).isSameInstanceAs(transformerError);
  }

  @Test
  void awaitOperation_executionExceptionWrappingNestedInterruptsAndTimeouts_handlesAllChains() {
    DeleteFolderRecursiveMetadata metadata =
        DeleteFolderRecursiveMetadata.newBuilder()
            .setCommonMetadata(
                CommonLongRunningOperationMetadata.newBuilder().setProgressPercent(65).build())
            .build();

    // 1. GAX CancelledException wrapping StatusRuntimeException(CANCELLED) with nested
    // InterruptedException -> cancels opFuture, restores interrupt, throws InterruptedIOException.
    InterruptedException nestedGrpcInterrupt = new InterruptedException("grpc call interrupted");
    StatusRuntimeException grpcCancelled =
        Status.CANCELLED.withCause(nestedGrpcInterrupt).asRuntimeException();
    CancelledException gaxCancelled =
        new CancelledException(
            "rpc cancelled", grpcCancelled, GrpcStatusCode.of(Status.Code.CANCELLED), false);
    AtomicBoolean interruptedDuringGaxCancel = new AtomicBoolean(true);
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> gaxCancelledOp =
        new FakeOperationFuture<Void, DeleteFolderRecursiveMetadata>(
            ApiFutures.immediateFuture(
                new FakeOperationSnapshot("operations/del-gax-cancel", false)),
            ApiFutures.immediateFuture(metadata)) {
          @Override
          public Void get() throws ExecutionException {
            throw new ExecutionException(gaxCancelled);
          }

          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            interruptedDuringGaxCancel.set(Thread.currentThread().isInterrupted());
            return super.cancel(mayInterruptIfRunning);
          }
        };

    InterruptedIOException fromGaxCancelled =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(gaxCancelledOp, "Recursive delete"));
    assertThat(gaxCancelledOp.wasCancelledWithInterrupt()).isTrue();
    assertThat(interruptedDuringGaxCancel.get()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(fromGaxCancelled).hasMessageThat().contains("Recursive delete was interrupted");
    assertThat(fromGaxCancelled).hasMessageThat().contains("operations/del-gax-cancel");
    assertThat(fromGaxCancelled).hasMessageThat().contains("progress=65%");
    assertThat(fromGaxCancelled).hasCauseThat().isSameInstanceAs(gaxCancelled);
    Thread.interrupted();

    // 2. Direct StatusRuntimeException(CANCELLED) wrapping ClosedByInterruptException -> cancels,
    // restores interrupt, throws InterruptedIOException.
    StatusRuntimeException statusWithChannelInterrupt =
        Status.CANCELLED.withCause(new ClosedByInterruptException()).asRuntimeException();
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> statusInterruptOp =
        FakeOperationFuture.failingWith(
            statusWithChannelInterrupt, "operations/del-status-int", metadata);
    InterruptedIOException fromStatusInterrupt =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(statusInterruptOp, "Recursive delete"));
    assertThat(statusInterruptOp.wasCancelledWithInterrupt()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(fromStatusInterrupt).hasMessageThat().contains("operations/del-status-int");
    assertThat(fromStatusInterrupt).hasCauseThat().isSameInstanceAs(statusWithChannelInterrupt);
    Thread.interrupted();

    // 3. Server-side CancelledException WITHOUT local thread interrupt -> maps to generic
    // IOException and does NOT set caller interrupt flag.
    CancelledException serverSideCancelled =
        new CancelledException(
            "cancelled by server",
            Status.CANCELLED.asRuntimeException(),
            GrpcStatusCode.of(Status.Code.CANCELLED),
            false);
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> serverCancelledOp =
        FakeOperationFuture.failingWith(serverSideCancelled, "operations/del-server-cancel");
    IOException fromServerCancelled =
        assertThrows(
            IOException.class,
            () -> StorageControlLro.awaitOperation(serverCancelledOp, "Recursive delete"));
    assertThat(fromServerCancelled).isNotInstanceOf(InterruptedIOException.class);
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
    assertThat(fromServerCancelled).hasCauseThat().isSameInstanceAs(serverSideCancelled);

    // 4. Plain IOException wrapping InterruptedException -> cancels, restores interrupt, throws
    // InterruptedIOException.
    IOException ioWrappingInterrupt =
        new IOException("oauth refresh interrupted", new InterruptedException("token refresh"));
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> ioWrapOp =
        FakeOperationFuture.failingWith(ioWrappingInterrupt, "operations/del-io-wrap");
    InterruptedIOException fromIoWrap =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(ioWrapOp, "Recursive delete"));
    assertThat(ioWrapOp.wasCancelledWithInterrupt()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(fromIoWrap).hasMessageThat().contains("operations/del-io-wrap");
    assertThat(fromIoWrap).hasCauseThat().isSameInstanceAs(ioWrappingInterrupt);
    Thread.interrupted();

    // 5. Composite IOException suppressing InterruptedException (with cyclic suppressed graph) ->
    // cycle-safe traversal detects interrupt, cancels, restores interrupt, throws
    // InterruptedIOException.
    IOException compositeIo = new IOException("primary transport error");
    IOException siblingIo = new IOException("cleanup error");
    siblingIo.addSuppressed(compositeIo);
    siblingIo.addSuppressed(new InterruptedException("suppressed interrupt"));
    compositeIo.addSuppressed(siblingIo);
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> suppressedInterruptOp =
        FakeOperationFuture.failingWith(compositeIo, "operations/del-suppressed");
    InterruptedIOException fromSuppressed =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(suppressedInterruptOp, "Recursive delete"));
    assertThat(suppressedInterruptOp.wasCancelledWithInterrupt()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    assertThat(fromSuppressed).hasCauseThat().isSameInstanceAs(compositeIo);
    Thread.interrupted();

    // 6. Direct non-timeout InterruptedIOException -> cancels, restores interrupt, rethrows exact
    // InterruptedIOException instance.
    InterruptedIOException directInterruptedIo = new InterruptedIOException("stream interrupted");
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> directInterruptedIoOp =
        FakeOperationFuture.failingWith(directInterruptedIo, "operations/del-direct-iio");
    InterruptedIOException rethrownInterruptedIo =
        assertThrows(
            InterruptedIOException.class,
            () -> StorageControlLro.awaitOperation(directInterruptedIoOp, "Recursive delete"));
    assertThat(rethrownInterruptedIo).isSameInstanceAs(directInterruptedIo);
    assertThat(directInterruptedIoOp.wasCancelledWithInterrupt()).isTrue();
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    Thread.interrupted();

    // 7. SocketTimeoutException (subclasses InterruptedIOException in JDK) -> treated as socket
    // timeout, NOT thread interrupt; rethrown as-is without setting interrupt status.
    SocketTimeoutException socketTimeout = new SocketTimeoutException("read timed out");
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> socketTimeoutOp =
        FakeOperationFuture.failingWith(socketTimeout, "operations/del-sock-timeout");
    SocketTimeoutException rethrownSocketTimeout =
        assertThrows(
            SocketTimeoutException.class,
            () -> StorageControlLro.awaitOperation(socketTimeoutOp, "Recursive delete"));
    assertThat(rethrownSocketTimeout).isSameInstanceAs(socketTimeout);
    assertThat(socketTimeoutOp.wasCancelledWithInterrupt()).isFalse();
    assertThat(Thread.currentThread().isInterrupted()).isFalse();

    // 8. Cyclic non-interrupt exception graph -> terminates cleanly without setting interrupt flag.
    RuntimeException cycleA = new RuntimeException("cycle-a");
    RuntimeException cycleB = new RuntimeException("cycle-b", cycleA);
    cycleA.addSuppressed(cycleB);
    IOException cyclicIo =
        assertThrows(
            IOException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(cycleA, null), "Recursive delete"));
    assertThat(cyclicIo).isNotInstanceOf(InterruptedIOException.class);
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test
  void awaitOperation_executionExceptionWrappingApiExceptions_mapsStatusCodesCorrectly() {
    NotFoundException notFound =
        new NotFoundException(
            "folder missing", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    FileNotFoundException fnfe =
        assertThrows(
            FileNotFoundException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(notFound, "operations/op-nf"),
                    "Rename folder gs://b/src/ to gs://b/dst/"));
    assertThat(fnfe).hasCauseThat().isSameInstanceAs(notFound);
    assertThat(fnfe).hasMessageThat().contains("Rename folder gs://b/src/ to gs://b/dst/ failed");
    assertThat(fnfe).hasMessageThat().contains("operations/op-nf");
    assertThat(fnfe).hasMessageThat().contains("folder missing");

    AlreadyExistsException alreadyExists =
        new AlreadyExistsException(
            "dest exists", null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
    FileAlreadyExistsException faee =
        assertThrows(
            FileAlreadyExistsException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(alreadyExists, "operations/op-ae"),
                    "Rename folder gs://b/src/ to gs://b/dst/"));
    assertThat(faee).hasCauseThat().isSameInstanceAs(alreadyExists);
    assertThat(faee.getFile()).isNull();
    assertThat(faee.getOtherFile()).isNull();
    assertThat(faee.getReason())
        .isEqualTo(
            "Rename folder gs://b/src/ to gs://b/dst/ failed (operation=operations/op-ae): dest"
                + " exists");
    assertThat(faee.getMessage()).isEqualTo(faee.getReason());

    // Null cause message + null operation name still formats "<operationDescription> failed"
    // cleanly with null getFile().
    AlreadyExistsException namelessAlreadyExists =
        new AlreadyExistsException(
            null, null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
    FileAlreadyExistsException namelessFaee =
        assertThrows(
            FileAlreadyExistsException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(namelessAlreadyExists, null),
                    "Rename folder gs://b/src/ to gs://b/dst/"));
    assertThat(namelessFaee.getFile()).isNull();
    assertThat(namelessFaee.getReason())
        .isEqualTo("Rename folder gs://b/src/ to gs://b/dst/ failed");
    assertThat(namelessFaee.getMessage())
        .isEqualTo("Rename folder gs://b/src/ to gs://b/dst/ failed");

    PermissionDeniedException permDenied =
        new PermissionDeniedException(
            "denied", null, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false);
    AccessDeniedException ade =
        assertThrows(
            AccessDeniedException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(permDenied, "operations/op-pd"),
                    "Delete folder gs://b/dir/"));
    assertThat(ade).hasCauseThat().isSameInstanceAs(permDenied);
    assertThat(ade.getFile()).isNull();
    assertThat(ade.getOtherFile()).isNull();
    assertThat(ade.getReason())
        .isEqualTo("Delete folder gs://b/dir/ failed (operation=operations/op-pd): denied");
    assertThat(ade.getMessage()).isEqualTo(ade.getReason());

    UnauthenticatedException unauth =
        new UnauthenticatedException(
            "unauth", null, GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), false);
    AccessDeniedException unauthAde =
        assertThrows(
            AccessDeniedException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(unauth, null), "Delete folder gs://b/dir/"));
    assertThat(unauthAde).hasCauseThat().isSameInstanceAs(unauth);
    assertThat(unauthAde.getFile()).isNull();
    assertThat(unauthAde.getOtherFile()).isNull();
    assertThat(unauthAde.getMessage()).isEqualTo("Delete folder gs://b/dir/ failed: unauth");

    FailedPreconditionException failedPrecondition =
        new FailedPreconditionException(
            "folder modified concurrently",
            null,
            GrpcStatusCode.of(Status.Code.FAILED_PRECONDITION),
            false);
    IOException precondIo =
        assertThrows(
            IOException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(failedPrecondition, "operations/op-fp"),
                    "Delete folder gs://b/dir/"));
    assertThat(precondIo).isNotInstanceOf(FileNotFoundException.class);
    assertThat(precondIo).isNotInstanceOf(FileAlreadyExistsException.class);
    assertThat(precondIo).isNotInstanceOf(AccessDeniedException.class);
    assertThat(precondIo).hasMessageThat().contains("Delete folder gs://b/dir/ failed");
    assertThat(precondIo).hasMessageThat().contains("operations/op-fp");
    assertThat(precondIo).hasCauseThat().isSameInstanceAs(failedPrecondition);
  }

  @Test
  void awaitOperation_executionExceptionWrappingStatusRuntimeAndIoExceptions_mapsCorrectly() {
    IOException directIo = new IOException("already an IOException");
    IOException rethrown =
        assertThrows(
            IOException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(directIo, null), "Delete folder"));
    assertThat(rethrown).isSameInstanceAs(directIo);

    StatusRuntimeException grpcNotFound =
        Status.NOT_FOUND.withDescription("missing").asRuntimeException();
    FileNotFoundException fnfe =
        assertThrows(
            FileNotFoundException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(grpcNotFound, null),
                    "Rename folder gs://b/src/ to gs://b/dst/"));
    assertThat(fnfe).hasCauseThat().isSameInstanceAs(grpcNotFound);
    assertThat(fnfe).hasMessageThat().contains("Rename folder gs://b/src/ to gs://b/dst/ failed");

    StatusRuntimeException grpcAlreadyExists = Status.ALREADY_EXISTS.asRuntimeException();
    FileAlreadyExistsException grpcFaee =
        assertThrows(
            FileAlreadyExistsException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(grpcAlreadyExists, "operations/op-grpc-ae"),
                    "Rename folder gs://b/src/ to gs://b/dst/"));
    assertThat(grpcFaee.getFile()).isNull();
    assertThat(grpcFaee.getOtherFile()).isNull();
    assertThat(grpcFaee)
        .hasMessageThat()
        .contains("Rename folder gs://b/src/ to gs://b/dst/ failed");
    assertThat(grpcFaee).hasMessageThat().contains("operations/op-grpc-ae");
    assertThat(grpcFaee).hasCauseThat().isSameInstanceAs(grpcAlreadyExists);

    StatusRuntimeException grpcPermDenied = Status.PERMISSION_DENIED.asRuntimeException();
    AccessDeniedException grpcAde =
        assertThrows(
            AccessDeniedException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(grpcPermDenied, null),
                    "Delete folder gs://b/dir/"));
    assertThat(grpcAde.getFile()).isNull();
    assertThat(grpcAde.getOtherFile()).isNull();
    assertThat(grpcAde).hasMessageThat().contains("Delete folder gs://b/dir/ failed");
    assertThat(grpcAde).hasCauseThat().isSameInstanceAs(grpcPermDenied);

    ApiException bareApiInternal =
        new ApiException("internal", null, GrpcStatusCode.of(Status.Code.INTERNAL), false);
    IOException genericIo =
        assertThrows(
            IOException.class,
            () ->
                StorageControlLro.awaitOperation(
                    FakeOperationFuture.failingWith(bareApiInternal, null), "Rename folder"));
    assertThat(genericIo).hasMessageThat().isEqualTo("Rename folder failed");
    assertThat(genericIo).hasCauseThat().isSameInstanceAs(bareApiInternal);
  }

  @Test
  void
      safeGetOperationNameAndNullPeekMetadata_handleIncompleteCancelledEmptyAndInterruptedStates() {
    assertThat(StorageControlLro.safeGetOperationName(null)).isEmpty();
    assertThat(StorageControlLro.safePeekMetadata(null)).isEmpty();

    // Incomplete initialFuture + null peekMetadata(): never blocks, never NPEs.
    SettableApiFuture<OperationSnapshot> pendingInitial = SettableApiFuture.create();
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> pendingOp =
        new FakeOperationFuture<>(pendingInitial, null);
    assertThat(StorageControlLro.safeGetOperationName(pendingOp)).isEmpty();
    assertThat(StorageControlLro.safePeekMetadata(pendingOp)).isEmpty();

    // Cancelled initialFuture.
    SettableApiFuture<OperationSnapshot> cancelledInitial = SettableApiFuture.create();
    cancelledInitial.cancel(true);
    FakeOperationFuture<Void, DeleteFolderRecursiveMetadata> cancelledOp =
        new FakeOperationFuture<>(cancelledInitial, null);
    assertThat(StorageControlLro.safeGetOperationName(cancelledOp)).isEmpty();

    // Failed initialFuture.
    ApiFuture<OperationSnapshot> failedInitial =
        ApiFutures.immediateFailedFuture(new RuntimeException("initial RPC failed"));
    assertThat(
            StorageControlLro.safeGetOperationName(new FakeOperationFuture<>(failedInitial, null)))
        .isEmpty();

    // Empty / null operation name in snapshot.
    ApiFuture<OperationSnapshot> emptyNameInitial =
        ApiFutures.immediateFuture(new FakeOperationSnapshot("", false));
    assertThat(
            StorageControlLro.safeGetOperationName(
                new FakeOperationFuture<>(emptyNameInitial, null)))
        .isEmpty();

    // Completed initialFuture while thread is pre-interrupted: reads uninterruptibly and preserves
    // the thread interrupt flag.
    ApiFuture<OperationSnapshot> namedInitial =
        ApiFutures.immediateFuture(new FakeOperationSnapshot("operations/op-safe", false));
    Thread.currentThread().interrupt();
    assertThat(
            StorageControlLro.safeGetOperationName(new FakeOperationFuture<>(namedInitial, null)))
        .hasValue("operations/op-safe");
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }

  @Test
  void hnsRecursiveDeleteException_exposesFolderIdAndOptionalOperationName() {
    GcsItemId folderId =
        GcsItemId.builder().setBucketName("test-bucket").setObjectName("parent/child/").build();
    IOException cause = new IOException("partial failure");

    HnsRecursiveDeleteException withOp =
        new HnsRecursiveDeleteException(folderId, "operations/del-1", "failed", cause);
    assertThat(withOp.getFolderId()).isEqualTo(folderId);
    assertThat(withOp.getOperationName()).hasValue("operations/del-1");
    assertThat(withOp).hasCauseThat().isSameInstanceAs(cause);

    HnsRecursiveDeleteException withoutOp =
        new HnsRecursiveDeleteException(folderId, "failed early", null);
    assertThat(withoutOp.getFolderId()).isEqualTo(folderId);
    assertThat(withoutOp.getOperationName()).isEmpty();
  }

  @Test
  void defaultPollAlgorithmOverloads_produceWorkingAlgorithms() {
    TimedRetryAlgorithm defaultAlg = StorageControlLro.createDefaultPollAlgorithm();
    TimedAttemptSettings first = defaultAlg.createFirstAttempt();
    assertThat(defaultAlg.shouldRetry(first)).isTrue();

    TimedRetryAlgorithm fourArgAlg =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            Duration.ofMillis(50), Duration.ofMillis(50), 0.05, Duration.ofMinutes(1));
    assertThat(fourArgAlg.shouldRetry(fourArgAlg.createFirstAttempt())).isTrue();
  }

  @Test
  void createLinearProportionalPollAlgorithm_foreverAndLargeDurations_saturateWithoutOverflow() {
    FakeApiClock clock = new FakeApiClock(0L);
    Duration forever = ChronoUnit.FOREVER.getDuration();

    // Sentinel totalTimeout and maxDelay saturate to Long.MAX_VALUE nanos without throwing
    // ArithmeticException inside RetrySettings.Builder.build().
    TimedRetryAlgorithm algorithm =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            Duration.ofMillis(50), forever, 0.05, forever, clock);
    TimedAttemptSettings first = algorithm.createFirstAttempt();
    assertThat(first.getGlobalSettings().getTotalTimeoutDuration())
        .isEqualTo(Duration.ofNanos(Long.MAX_VALUE));
    assertThat(first.getGlobalSettings().getMaxRetryDelayDuration())
        .isEqualTo(Duration.ofNanos(Long.MAX_VALUE));
    assertThat(algorithm.shouldRetry(first)).isTrue();

    clock.setNanos(Duration.ofSeconds(10).toNanos());
    TimedAttemptSettings next = algorithm.createNextAttempt(first);
    assertThat(next.getRetryDelayDuration()).isEqualTo(Duration.ofMillis(500));
    assertThat(algorithm.shouldRetry(next)).isTrue();

    // Sentinel minDelay also saturates cleanly without ArithmeticException.
    TimedRetryAlgorithm allForever =
        StorageControlLro.createLinearProportionalPollAlgorithm(
            forever, forever, 0.05, forever, clock);
    TimedAttemptSettings allForeverFirst = allForever.createFirstAttempt();
    assertThat(allForeverFirst.getGlobalSettings().getInitialRetryDelayDuration())
        .isEqualTo(Duration.ofNanos(Long.MAX_VALUE));
  }

  private static final class FakeApiClock implements ApiClock {
    private final AtomicLong nanos;

    private FakeApiClock(long initialNanos) {
      this.nanos = new AtomicLong(initialNanos);
    }

    void advance(Duration delta) {
      nanos.addAndGet(delta.toNanos());
    }

    void setNanos(long value) {
      nanos.set(value);
    }

    @Override
    public long nanoTime() {
      return nanos.get();
    }

    @Override
    public long millisTime() {
      return TimeUnit.NANOSECONDS.toMillis(nanos.get());
    }
  }

  private static final class FakeOperationSnapshot implements OperationSnapshot {
    @Nullable private final String name;
    private final boolean done;
    @Nullable private final Object metadata;

    private FakeOperationSnapshot(@Nullable String name, boolean done) {
      this(name, done, null);
    }

    private FakeOperationSnapshot(@Nullable String name, boolean done, @Nullable Object metadata) {
      this.name = name;
      this.done = done;
      this.metadata = metadata;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Object getMetadata() {
      return metadata;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public Object getResponse() {
      return Folder.getDefaultInstance();
    }

    @Override
    public StatusCode getErrorCode() {
      return GrpcStatusCode.of(Status.Code.OK);
    }

    @Override
    public String getErrorMessage() {
      return "";
    }
  }

  private static class FakeOperationFuture<RespT, MetaT> implements OperationFuture<RespT, MetaT> {
    private final ApiFuture<OperationSnapshot> initialFuture;
    @Nullable private volatile ApiFuture<MetaT> peekedMetadata;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicBoolean cancelledWithInterrupt = new AtomicBoolean(false);

    FakeOperationFuture(
        ApiFuture<OperationSnapshot> initialFuture, @Nullable ApiFuture<MetaT> peekedMetadata) {
      this.initialFuture = initialFuture;
      this.peekedMetadata = peekedMetadata;
    }

    static <RespT, MetaT> FakeOperationFuture<RespT, MetaT> failingWith(
        Throwable cause, @Nullable String opName) {
      return failingWith(cause, opName, null);
    }

    static <RespT, MetaT> FakeOperationFuture<RespT, MetaT> failingWith(
        Throwable cause, @Nullable String opName, @Nullable MetaT metadata) {
      ApiFuture<OperationSnapshot> initial =
          opName != null
              ? ApiFutures.immediateFuture(new FakeOperationSnapshot(opName, true))
              : SettableApiFuture.create();
      ApiFuture<MetaT> metaFuture = metadata != null ? ApiFutures.immediateFuture(metadata) : null;
      return new FakeOperationFuture<RespT, MetaT>(initial, metaFuture) {
        @Override
        public RespT get() throws ExecutionException {
          throw new ExecutionException(cause);
        }
      };
    }

    boolean wasCancelledWithInterrupt() {
      return cancelledWithInterrupt.get();
    }

    @Override
    public String getName() {
      throw new AssertionError("OperationFuture.getName() blocks and must never be called");
    }

    @Override
    public ApiFuture<OperationSnapshot> getInitialFuture() {
      return initialFuture;
    }

    @Override
    public RetryingFuture<OperationSnapshot> getPollingFuture() {
      return null;
    }

    @Override
    @Nullable
    public ApiFuture<MetaT> peekMetadata() {
      return peekedMetadata;
    }

    @Override
    public ApiFuture<MetaT> getMetadata() {
      return peekedMetadata;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {}

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled.set(true);
      if (mayInterruptIfRunning) {
        cancelledWithInterrupt.set(true);
      }
      // Mirror GAX OperationFutureImpl + BasicRetryingFuture: cancelling the operation future
      // replaces latestCompletedAttemptResult with a cancelled future, invalidating peekMetadata().
      SettableApiFuture<MetaT> cancelledMetadata = SettableApiFuture.create();
      cancelledMetadata.cancel(mayInterruptIfRunning);
      this.peekedMetadata = cancelledMetadata;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }

    @Override
    public boolean isDone() {
      return cancelled.get();
    }

    @Override
    public RespT get() throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public RespT get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return get();
    }
  }
}
