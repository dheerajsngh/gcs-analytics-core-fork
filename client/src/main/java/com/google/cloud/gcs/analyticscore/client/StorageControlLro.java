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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiClock;
import com.google.api.core.ApiFuture;
import com.google.api.core.NanoClock;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.longrunning.OperationSnapshot;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.RetryingContext;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.api.gax.retrying.TimedRetryAlgorithm;
import com.google.api.gax.retrying.TimedRetryAlgorithmWithContext;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnauthenticatedException;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.storage.control.v2.CommonLongRunningOperationMetadata;
import com.google.storage.control.v2.DeleteFolderRecursiveMetadata;
import com.google.storage.control.v2.RenameFolderMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Utilities for polling and awaiting Google Cloud Storage Control v2 long-running operations
 * (LROs).
 */
final class StorageControlLro {

  static final Duration DEFAULT_MIN_POLL_DELAY = Duration.ofMillis(50);
  static final Duration DEFAULT_MAX_POLL_DELAY = Duration.ofSeconds(30);
  static final double DEFAULT_POLL_PROPORTION = 0.05;
  static final Duration DEFAULT_TOTAL_TIMEOUT = Duration.ofMinutes(15);

  private StorageControlLro() {}

  /**
   * Creates the default linear-proportional LRO polling algorithm ({@code [50ms, 30s]} at {@code
   * 5%} of elapsed time, {@code 15m} total timeout).
   */
  static TimedRetryAlgorithm createDefaultPollAlgorithm() {
    return createLinearProportionalPollAlgorithm(
        DEFAULT_MIN_POLL_DELAY,
        DEFAULT_MAX_POLL_DELAY,
        DEFAULT_POLL_PROPORTION,
        DEFAULT_TOTAL_TIMEOUT);
  }

  /**
   * Creates a linear-proportional LRO polling algorithm backed by {@link
   * NanoClock#getDefaultClock()}.
   */
  static TimedRetryAlgorithm createLinearProportionalPollAlgorithm(
      Duration minDelay, Duration maxDelay, double proportion, Duration totalTimeout) {
    return createLinearProportionalPollAlgorithm(
        minDelay, maxDelay, proportion, totalTimeout, NanoClock.getDefaultClock());
  }

  /**
   * Creates a linear-proportional LRO polling algorithm that sets each poll delay to {@code
   * proportion * elapsed} clamped to {@code [minDelay, maxDelay]} and capped to the remaining
   * budget before {@code totalTimeout}.
   */
  static TimedRetryAlgorithm createLinearProportionalPollAlgorithm(
      Duration minDelay,
      Duration maxDelay,
      double proportion,
      Duration totalTimeout,
      ApiClock clock) {
    checkNotNull(minDelay, "minDelay must not be null");
    checkNotNull(maxDelay, "maxDelay must not be null");
    checkNotNull(totalTimeout, "totalTimeout must not be null");
    checkNotNull(clock, "clock must not be null");
    checkArgument(
        !minDelay.isNegative() && !minDelay.isZero(), "minDelay must be > 0, got %s", minDelay);
    checkArgument(
        maxDelay.compareTo(minDelay) >= 0,
        "maxDelay (%s) must be >= minDelay (%s)",
        maxDelay,
        minDelay);
    checkArgument(
        Double.isFinite(proportion) && proportion > 0.0,
        "proportion must be finite and > 0, got %s",
        proportion);
    checkArgument(
        !totalTimeout.isNegative() && !totalTimeout.isZero(),
        "totalTimeout must be > 0, got %s",
        totalTimeout);
    return new LinearProportionalPollAlgorithm(minDelay, maxDelay, proportion, totalTimeout, clock);
  }

  /**
   * Awaits completion of {@code opFuture} using the polling algorithm's configured timeout,
   * translating interruptions, cancellations, and gRPC/GAX failures into {@link IOException}
   * subtypes.
   */
  static <RespT, MetaT> RespT awaitOperation(
      OperationFuture<RespT, MetaT> opFuture, String operationDescription) throws IOException {
    return awaitOperation(opFuture, operationDescription, null);
  }

  /**
   * Awaits completion of {@code opFuture}, applying an explicit {@code timeout} when non-null or
   * delegating to {@link OperationFuture#get()} when {@code timeout} is {@code null}.
   */
  static <RespT, MetaT> RespT awaitOperation(
      OperationFuture<RespT, MetaT> opFuture,
      String operationDescription,
      @Nullable Duration timeout)
      throws IOException {
    checkNotNull(opFuture, "opFuture must not be null");
    checkNotNull(operationDescription, "operationDescription must not be null");
    if (timeout != null) {
      checkArgument(!timeout.isNegative(), "timeout must not be negative, got %s", timeout);
    }
    try {
      if (timeout == null) {
        return opFuture.get();
      }
      long timeoutMillis = timeout.isZero() ? 0L : Math.max(1L, saturatedToMillis(timeout));
      return opFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      String message;
      try {
        try {
          message = formatInterruptedMessage(operationDescription, opFuture);
        } finally {
          opFuture.cancel(true);
        }
      } finally {
        Thread.currentThread().interrupt();
      }
      throw createInterruptedIoException(message, e);
    } catch (TimeoutException | CancellationException e) {
      String message;
      try {
        message = formatTimeoutMessage(operationDescription, timeout, opFuture);
      } finally {
        opFuture.cancel(true);
      }
      throw new IOException(message, e);
    } catch (ExecutionException e) {
      throw translateExecutionException(e, opFuture, operationDescription, timeout);
    }
  }

  /**
   * Non-blocking helper that extracts the server operation name ONLY if {@link
   * OperationFuture#getInitialFuture()} is already completed and not cancelled, and also verifies
   * {@link OperationFuture#peekMetadata()} is null-safe. Never calls {@link
   * OperationFuture#getName()}, which blocks on {@code initialFuture.get()}.
   */
  static Optional<String> safeGetOperationName(@Nullable OperationFuture<?, ?> opFuture) {
    if (opFuture == null) {
      return Optional.empty();
    }
    // Touch peekMetadata() safely so callers and diagnostic paths never trip on a null return.
    safePeekMetadata(opFuture);
    try {
      ApiFuture<OperationSnapshot> initialFuture = opFuture.getInitialFuture();
      if (initialFuture != null && initialFuture.isDone() && !initialFuture.isCancelled()) {
        OperationSnapshot snapshot = Uninterruptibles.getUninterruptibly(initialFuture);
        if (snapshot != null) {
          String name = snapshot.getName();
          if (name != null && !name.isEmpty()) {
            return Optional.of(name);
          }
        }
      }
    } catch (Throwable ignored) {
      // Never allow diagnostic inspection to block or mask the primary LRO error.
    }
    return Optional.empty();
  }

  /**
   * Non-blocking helper that inspects {@link OperationFuture#peekMetadata()} null-safely (GAX
   * returns {@code null} from {@code peekMetadata()} before the first poll attempt completes).
   */
  static <MetaT> Optional<MetaT> safePeekMetadata(@Nullable OperationFuture<?, MetaT> opFuture) {
    if (opFuture == null) {
      return Optional.empty();
    }
    try {
      ApiFuture<MetaT> metadataFuture = opFuture.peekMetadata();
      if (metadataFuture != null && metadataFuture.isDone() && !metadataFuture.isCancelled()) {
        return Optional.ofNullable(Uninterruptibles.getUninterruptibly(metadataFuture));
      }
    } catch (Throwable ignored) {
      // Metadata unavailable or failed; ignore in diagnostic path.
    }
    return Optional.empty();
  }

  private static IOException translateExecutionException(
      ExecutionException e,
      OperationFuture<?, ?> opFuture,
      String operationDescription,
      @Nullable Duration timeout) {
    Throwable cause = e.getCause();
    if (cause == null) {
      return new IOException(formatFailureMessage(operationDescription, opFuture), e);
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    if (containsThreadInterruption(cause)) {
      String message;
      // Clear any pre-existing interrupt bit while snapshotting diagnostics and cancelling
      // opFuture; the outer finally unconditionally restores the interrupt status.
      Thread.interrupted();
      try {
        try {
          message = formatInterruptedMessage(operationDescription, opFuture);
        } finally {
          opFuture.cancel(true);
        }
      } finally {
        Thread.currentThread().interrupt();
      }
      if (isNonTimeoutInterruptedIoException(cause)) {
        return (InterruptedIOException) cause;
      }
      return createInterruptedIoException(message, cause);
    }
    if (cause instanceof CancellationException) {
      String message;
      try {
        message = formatTimeoutMessage(operationDescription, timeout, opFuture);
      } finally {
        opFuture.cancel(true);
      }
      return new IOException(message, cause);
    }
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    if (cause instanceof ApiException) {
      return translateApiException((ApiException) cause, opFuture, operationDescription);
    }
    if (cause instanceof StatusRuntimeException) {
      return translateStatusRuntimeException(
          (StatusRuntimeException) cause, opFuture, operationDescription);
    }
    return new IOException(formatFailureMessage(operationDescription, opFuture), cause);
  }

  private static IOException translateApiException(
      ApiException cause, OperationFuture<?, ?> opFuture, String operationDescription) {
    StatusCode statusCode = cause.getStatusCode();
    StatusCode.Code code = statusCode != null ? statusCode.getCode() : null;
    if (cause instanceof NotFoundException || code == StatusCode.Code.NOT_FOUND) {
      return createFileNotFoundException(operationDescription, opFuture, cause);
    }
    if (cause instanceof AlreadyExistsException || code == StatusCode.Code.ALREADY_EXISTS) {
      return createFileAlreadyExistsException(operationDescription, opFuture, cause);
    }
    if (cause instanceof PermissionDeniedException
        || cause instanceof UnauthenticatedException
        || code == StatusCode.Code.PERMISSION_DENIED
        || code == StatusCode.Code.UNAUTHENTICATED) {
      return createAccessDeniedException(operationDescription, opFuture, cause);
    }
    return new IOException(formatFailureMessage(operationDescription, opFuture), cause);
  }

  private static IOException translateStatusRuntimeException(
      StatusRuntimeException cause, OperationFuture<?, ?> opFuture, String operationDescription) {
    Status status = cause.getStatus();
    Status.Code code = status != null ? status.getCode() : null;
    if (code == Status.Code.NOT_FOUND) {
      return createFileNotFoundException(operationDescription, opFuture, cause);
    }
    if (code == Status.Code.ALREADY_EXISTS) {
      return createFileAlreadyExistsException(operationDescription, opFuture, cause);
    }
    if (code == Status.Code.PERMISSION_DENIED || code == Status.Code.UNAUTHENTICATED) {
      return createAccessDeniedException(operationDescription, opFuture, cause);
    }
    return new IOException(formatFailureMessage(operationDescription, opFuture), cause);
  }

  private static FileNotFoundException createFileNotFoundException(
      String operationDescription, OperationFuture<?, ?> opFuture, Throwable cause) {
    FileNotFoundException exception =
        new FileNotFoundException(formatFailureDetail(operationDescription, opFuture, cause));
    exception.initCause(cause);
    return exception;
  }

  private static FileAlreadyExistsException createFileAlreadyExistsException(
      String operationDescription, OperationFuture<?, ?> opFuture, Throwable cause) {
    FileAlreadyExistsException exception =
        new FileAlreadyExistsException(
            null, null, formatFailureDetail(operationDescription, opFuture, cause));
    exception.initCause(cause);
    return exception;
  }

  private static AccessDeniedException createAccessDeniedException(
      String operationDescription, OperationFuture<?, ?> opFuture, Throwable cause) {
    AccessDeniedException exception =
        new AccessDeniedException(
            null, null, formatFailureDetail(operationDescription, opFuture, cause));
    exception.initCause(cause);
    return exception;
  }

  private static InterruptedIOException createInterruptedIoException(
      String message, Throwable cause) {
    InterruptedIOException exception = new InterruptedIOException(message);
    exception.initCause(cause);
    return exception;
  }

  /**
   * Traverses the causal and suppressed-exception graph of {@code root} cycle-safely to detect
   * genuine thread interruptions ({@link InterruptedException}, {@link ClosedByInterruptException},
   * or a non-timeout {@link InterruptedIOException}). {@link SocketTimeoutException} subclasses
   * {@link InterruptedIOException} in the JDK for historical reasons despite representing a socket
   * timeout rather than a thread interrupt, and is therefore excluded.
   */
  private static boolean containsThreadInterruption(@Nullable Throwable root) {
    if (root == null) {
      return false;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> stack = new ArrayDeque<>();
    visited.add(root);
    stack.push(root);
    while (!stack.isEmpty()) {
      Throwable current = stack.pop();
      if (isThreadInterruption(current)) {
        return true;
      }
      Throwable nextCause = current.getCause();
      if (nextCause != null && visited.add(nextCause)) {
        stack.push(nextCause);
      }
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null && visited.add(suppressed)) {
          stack.push(suppressed);
        }
      }
    }
    return false;
  }

  private static boolean isThreadInterruption(Throwable t) {
    return t instanceof InterruptedException
        || t instanceof ClosedByInterruptException
        || isNonTimeoutInterruptedIoException(t);
  }

  private static boolean isNonTimeoutInterruptedIoException(Throwable t) {
    return t instanceof InterruptedIOException && !(t instanceof SocketTimeoutException);
  }

  private static String formatInterruptedMessage(
      String operationDescription, OperationFuture<?, ?> opFuture) {
    return operationDescription + " was interrupted" + formatDiagnosticSuffix(opFuture);
  }

  private static String formatTimeoutMessage(
      String operationDescription, @Nullable Duration timeout, OperationFuture<?, ?> opFuture) {
    String base =
        timeout != null
            ? operationDescription + " timed out after " + timeout
            : operationDescription + " timed out";
    return base + formatDiagnosticSuffix(opFuture);
  }

  private static String formatFailureMessage(
      String operationDescription, OperationFuture<?, ?> opFuture) {
    return operationDescription + " failed" + formatDiagnosticSuffix(opFuture);
  }

  private static String formatFailureDetail(
      String operationDescription, OperationFuture<?, ?> opFuture, Throwable cause) {
    String base = formatFailureMessage(operationDescription, opFuture);
    String causeMessage = cause.getMessage();
    return (causeMessage != null && !causeMessage.isEmpty()) ? base + ": " + causeMessage : base;
  }

  private static String formatDiagnosticSuffix(OperationFuture<?, ?> opFuture) {
    List<String> details = new ArrayList<>(2);
    safeGetOperationName(opFuture).ifPresent(name -> details.add("operation=" + name));
    safePeekMetadata(opFuture)
        .ifPresent(
            meta ->
                extractProgressPercent(meta)
                    .ifPresent(progress -> details.add("progress=" + progress + "%")));
    return details.isEmpty() ? "" : " (" + String.join(", ", details) + ")";
  }

  private static OptionalInt extractProgressPercent(Object metadata) {
    if (metadata instanceof DeleteFolderRecursiveMetadata) {
      DeleteFolderRecursiveMetadata deleteMeta = (DeleteFolderRecursiveMetadata) metadata;
      if (deleteMeta.hasCommonMetadata()) {
        return OptionalInt.of(deleteMeta.getCommonMetadata().getProgressPercent());
      }
    } else if (metadata instanceof RenameFolderMetadata) {
      RenameFolderMetadata renameMeta = (RenameFolderMetadata) metadata;
      if (renameMeta.hasCommonMetadata()) {
        return OptionalInt.of(renameMeta.getCommonMetadata().getProgressPercent());
      }
    } else if (metadata instanceof CommonLongRunningOperationMetadata) {
      return OptionalInt.of(((CommonLongRunningOperationMetadata) metadata).getProgressPercent());
    }
    return OptionalInt.empty();
  }

  private static long saturatedToNanos(Duration duration) {
    try {
      return duration.toNanos();
    } catch (ArithmeticException e) {
      return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
  }

  private static long saturatedToMillis(Duration duration) {
    try {
      return duration.toMillis();
    } catch (ArithmeticException e) {
      return duration.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
  }

  private static final class LinearProportionalPollAlgorithm
      implements TimedRetryAlgorithmWithContext {
    private final long minDelayNanos;
    private final long maxDelayNanos;
    private final double proportion;
    private final Duration totalTimeout;
    private final long totalTimeoutNanos;
    private final ApiClock clock;
    private final RetrySettings globalSettings;
    private volatile long lastFirstAttemptStartNanos;

    private LinearProportionalPollAlgorithm(
        Duration minDelay,
        Duration maxDelay,
        double proportion,
        Duration totalTimeout,
        ApiClock clock) {
      this.minDelayNanos = saturatedToNanos(minDelay);
      this.maxDelayNanos = saturatedToNanos(maxDelay);
      this.proportion = proportion;
      this.totalTimeout = totalTimeout;
      this.totalTimeoutNanos = saturatedToNanos(totalTimeout);
      this.clock = clock;
      this.globalSettings =
          RetrySettings.newBuilder()
              .setInitialRetryDelayDuration(Duration.ofNanos(this.minDelayNanos))
              .setMaxRetryDelayDuration(Duration.ofNanos(this.maxDelayNanos))
              .setRetryDelayMultiplier(1.0)
              .setInitialRpcTimeoutDuration(Duration.ZERO)
              .setRpcTimeoutMultiplier(1.0)
              .setMaxRpcTimeoutDuration(Duration.ZERO)
              .setTotalTimeoutDuration(Duration.ofNanos(this.totalTimeoutNanos))
              .build();
      this.lastFirstAttemptStartNanos = clock.nanoTime();
    }

    @Override
    public TimedAttemptSettings createFirstAttempt() {
      long startNanos = clock.nanoTime();
      this.lastFirstAttemptStartNanos = startNanos;
      return TimedAttemptSettings.newBuilder()
          .setGlobalSettings(globalSettings)
          .setRetryDelayDuration(Duration.ZERO)
          .setRandomizedRetryDelayDuration(Duration.ZERO)
          .setRpcTimeoutDuration(Duration.ZERO)
          .setAttemptCount(0)
          .setOverallAttemptCount(0)
          .setFirstAttemptStartTimeNanos(startNanos)
          .build();
    }

    @Override
    public TimedAttemptSettings createFirstAttempt(RetryingContext context) {
      return createFirstAttempt();
    }

    @Override
    public TimedAttemptSettings createNextAttempt(TimedAttemptSettings previousSettings) {
      checkNotNull(previousSettings, "previousSettings must not be null");
      long startNanos = resolveStartNanos(previousSettings);
      long rawElapsedNanos = clock.nanoTime() - startNanos;
      long elapsedNanos = Math.max(0L, rawElapsedNanos);
      long proportionalNanos = Math.round(elapsedNanos * proportion);
      long clampedNanos = Math.max(minDelayNanos, Math.min(maxDelayNanos, proportionalNanos));
      long remainingNanos =
          rawElapsedNanos >= totalTimeoutNanos ? 0L : totalTimeoutNanos - elapsedNanos;
      long delayNanos = Math.min(clampedNanos, remainingNanos);
      Duration delay = Duration.ofNanos(delayNanos);
      RetrySettings settings =
          previousSettings.getGlobalSettings() != null
              ? previousSettings.getGlobalSettings()
              : globalSettings;
      return TimedAttemptSettings.newBuilder()
          .setGlobalSettings(settings)
          .setRetryDelayDuration(delay)
          .setRandomizedRetryDelayDuration(delay)
          .setRpcTimeoutDuration(Duration.ZERO)
          .setAttemptCount(previousSettings.getAttemptCount() + 1)
          .setOverallAttemptCount(previousSettings.getOverallAttemptCount() + 1)
          .setFirstAttemptStartTimeNanos(startNanos)
          .build();
    }

    @Override
    public TimedAttemptSettings createNextAttempt(
        RetryingContext context, TimedAttemptSettings previousSettings) {
      return createNextAttempt(previousSettings);
    }

    @Override
    public boolean shouldRetry(TimedAttemptSettings nextAttemptSettings)
        throws CancellationException {
      checkNotNull(nextAttemptSettings, "nextAttemptSettings must not be null");
      long startNanos = resolveStartNanos(nextAttemptSettings);
      if (clock.nanoTime() - startNanos >= totalTimeoutNanos) {
        throw new CancellationException("LRO polling timed out after " + totalTimeout);
      }
      return true;
    }

    @Override
    public boolean shouldRetry(RetryingContext context, TimedAttemptSettings nextAttemptSettings)
        throws CancellationException {
      return shouldRetry(nextAttemptSettings);
    }

    private long resolveStartNanos(TimedAttemptSettings settings) {
      // AutoValue_TimedAttemptSettings always populates a non-null globalSettings; a null value
      // only occurs when a synthetic Mockito mock is passed in unit tests.
      return settings.getGlobalSettings() != null
          ? settings.getFirstAttemptStartTimeNanos()
          : lastFirstAttemptStartNanos;
    }
  }
}
