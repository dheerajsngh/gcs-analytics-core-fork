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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchExecutor provides a means to manually batch requests using a thread pool. Execution is
 * performed by the underlying requestsExecutor ExecutorService.
 */
final class BatchExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(BatchExecutor.class);

  // TODO: Benchmark and determine the optimal default batch executor configuration; currently
  // mirrors hadoop-connectors defaults.
  /** Keep-alive time for core threads. */
  private static final long KEEP_ALIVE_SECONDS = 10L;

  /** Multiplier bounding the task queue size relative to thread count. */
  private static final int QUEUE_CAPACITY_PER_THREAD = 20;

  /** Maximum number of suppressed failures attached to the primary batch {@link IOException}. */
  static final int MAX_SUPPRESSED = 100;

  /**
   * Grace period for thread pool shutdown. Since awaitRequestsCompletion() already waits for all
   * tasks to complete, threads should be idle and terminate instantaneously.
   */
  private static final long SHUTDOWN_GRACE_PERIOD_SECONDS = 1L;

  private final ExecutorService requestsExecutor;
  private final int harvestThreshold;

  private final Object shutdownLock = new Object();

  private final Object futuresLock = new Object();
  private final ArrayDeque<Future<Void>> responseFutures;
  private int activeQueueCalls;
  private int activePolledFutures;
  private boolean shutdownInitiated;
  private boolean closed;
  private boolean shutdownCompleted;

  private final Object failureLock = new Object();
  private boolean acceptFailures = true;
  @Nullable private IOException primaryFailure;
  @Nullable private Set<Throwable> seenFailures;
  private int suppressedCount;
  private boolean restoreInterruptOnAwait;

  BatchExecutor(int numThreads) {
    checkArgument(numThreads >= 0, "numThreads must be non-negative, got %s", numThreads);
    checkArgument(
        numThreads <= GcsClientOptions.MAX_BATCH_THREADS,
        "numThreads must be at most %s, got %s",
        GcsClientOptions.MAX_BATCH_THREADS,
        numThreads);
    this.requestsExecutor =
        numThreads == 0 ? newDirectExecutorService() : newRequestExecutor(numThreads);
    this.harvestThreshold = numThreads == 0 ? 1 : numThreads * (QUEUE_CAPACITY_PER_THREAD + 2);
    this.responseFutures = new ArrayDeque<>(this.harvestThreshold);
  }

  @VisibleForTesting
  static ThreadPoolExecutor newRequestExecutor(int numThreads) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            /* corePoolSize= */ numThreads,
            /* maximumPoolSize= */ numThreads,
            KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads * QUEUE_CAPACITY_PER_THREAD),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-batch-executor-pool-%d")
                .setDaemon(true)
                .build());
    executor.allowCoreThreadTimeOut(true);
    executor.setRejectedExecutionHandler(
        new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (e.isShutdown()) {
              throw new IllegalStateException(
                  "requestExecutor should not be terminated to queue request");
            }
            r.run();
          }
        });
    return executor;
  }

  /** Adds a task to the execution queue. */
  public <T> void queue(Callable<T> task, @Nullable FutureCallback<? super T> callback) {
    checkNotNull(task, "task should not be null");
    synchronized (futuresLock) {
      checkState(
          !shutdownInitiated
              && !closed
              && !requestsExecutor.isShutdown()
              && !requestsExecutor.isTerminated(),
          "requestExecutor should not be terminated to queue request");
      activeQueueCalls++;
    }

    Thread callerThread = Thread.currentThread();
    boolean callerWasInterrupted = Thread.interrupted();
    try {
      boolean[] inSubmit = {true};
      boolean[] ranOnCallerThread = {false};
      Future<Void> future;
      try {
        future =
            requestsExecutor.submit(
                () -> {
                  if (Thread.currentThread() == callerThread && inSubmit[0]) {
                    ranOnCallerThread[0] = true;
                  }
                  execute(task, callback);
                  return null;
                });
      } catch (RejectedExecutionException e) {
        throw new IllegalStateException(
            "requestExecutor should not be terminated to queue request", e);
      } finally {
        inSubmit[0] = false;
      }
      boolean callerTaskSetInterrupt = ranOnCallerThread[0] && callerThread.isInterrupted();
      if (callerTaskSetInterrupt) {
        // A caller-run task (DirectExecutorService or CallerRunsPolicy) or its Future wrapper set
        // the caller thread's interrupt flag. Defer restoring it until awaitRequestsCompletion() so
        // subsequent queue() calls and sibling waits are not spuriously interrupted.
        Thread.interrupted();
      }

      boolean cancelLateFuture = false;
      boolean hasPendingHarvest = false;
      List<Future<Void>> doneFutures = null;
      synchronized (futuresLock) {
        if (closed) {
          cancelLateFuture = true;
        } else {
          boolean wasEmpty = responseFutures.isEmpty();
          responseFutures.addLast(future);
          if (responseFutures.size() >= harvestThreshold || callerTaskSetInterrupt) {
            doneFutures = drainDoneFuturesLocked();
          }
          if (wasEmpty && activePolledFutures > 0 && !responseFutures.isEmpty()) {
            futuresLock.notifyAll();
          }
          hasPendingHarvest = doneFutures != null || callerTaskSetInterrupt;
          if (hasPendingHarvest) {
            activePolledFutures++;
          }
        }
      }
      if (cancelLateFuture) {
        future.cancel(true);
        if (callerTaskSetInterrupt) {
          callerThread.interrupt();
        }
      } else if (hasPendingHarvest) {
        try {
          if (callerTaskSetInterrupt) {
            setRestoreInterruptOnAwait();
          }
          if (doneFutures != null) {
            for (Future<Void> doneFuture : doneFutures) {
              harvestDoneFuture(doneFuture);
            }
          }
        } finally {
          synchronized (futuresLock) {
            activePolledFutures--;
            if (activePolledFutures == 0) {
              futuresLock.notifyAll();
            }
          }
        }
      }
    } finally {
      synchronized (futuresLock) {
        activeQueueCalls--;
        if (activeQueueCalls == 0) {
          futuresLock.notifyAll();
        }
      }
      if (callerWasInterrupted) {
        callerThread.interrupt();
      }
    }
  }

  @Nullable
  private List<Future<Void>> drainDoneFuturesLocked() {
    List<Future<Void>> doneFutures = null;
    int count = responseFutures.size();
    for (int i = 0; i < count; i++) {
      Future<Void> candidate = responseFutures.pollFirst();
      if (candidate != null) {
        if (candidate.isDone()) {
          if (doneFutures == null) {
            doneFutures = new ArrayList<>();
          }
          doneFutures.add(candidate);
        } else {
          responseFutures.addLast(candidate);
        }
      }
    }
    return doneFutures;
  }

  private void harvestDoneFuture(Future<Void> doneFuture) {
    boolean interruptedBeforeGet = Thread.currentThread().isInterrupted();
    try {
      getFromFuture(doneFuture);
    } catch (IOException e) {
      boolean taskInterrupted = isTaskInterruptedException(e);
      if (taskInterrupted && !interruptedBeforeGet) {
        // Defer restoring interrupt set by getFromFuture() until awaitRequestsCompletion()
        // finishes draining so subsequent queue() calls or sibling waits are not disrupted.
        Thread.interrupted();
      }
      recordFailure(e, taskInterrupted);
    }
  }

  private static boolean isTaskInterruptedException(IOException e) {
    return e instanceof InterruptedIOException && e.getCause() instanceof InterruptedException;
  }

  private static <T> void execute(Callable<T> task, @Nullable FutureCallback<? super T> callback)
      throws Exception {
    T result;
    try {
      result = task.call();
    } catch (Exception e) {
      if (callback != null) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        try {
          callback.onFailure(e);
        } catch (RuntimeException | Error callbackThrowable) {
          if (callbackThrowable != e) {
            callbackThrowable.addSuppressed(e);
          }
          throw callbackThrowable;
        }
        return;
      }
      throw e;
    } catch (Throwable throwable) {
      if (callback != null) {
        try {
          callback.onFailure(throwable);
        } catch (Throwable callbackThrowable) {
          if (throwable != callbackThrowable) {
            throwable.addSuppressed(callbackThrowable);
          }
        }
      }
      throw throwable;
    }
    if (callback != null) {
      callback.onSuccess(result);
    }
  }

  /**
   * Checks if the underlying executor has any active or queued tasks. This is used to detect
   * stalling issues in jobs.
   */
  public boolean isIdle() {
    if (requestsExecutor instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) requestsExecutor;
      return executor.getActiveCount() == 0 && executor.getQueue().isEmpty();
    }
    // The executor is a DirectExecutorService, it is always idle because there are no child
    // threads.
    return true;
  }

  @VisibleForTesting
  int getPendingFuturesCount() {
    synchronized (futuresLock) {
      return responseFutures.size();
    }
  }

  @VisibleForTesting
  boolean isShutdownCompleted() {
    synchronized (futuresLock) {
      return shutdownCompleted;
    }
  }

  /** Awaits completion of all queued tasks and shuts down the executor. */
  public void shutdown() throws IOException {
    synchronized (shutdownLock) {
      synchronized (futuresLock) {
        if (shutdownCompleted) {
          return;
        }
      }
      try {
        drainPendingFutures();
        closeSubmissionGateAndAwaitActiveQueues();
        drainPendingFutures();
        checkState(getPendingFuturesCount() == 0, "responseFutures should be empty after await");
        throwIfFailedAndRestoreInterrupt();
      } finally {
        closeAndCancelRemainingFutures();
        requestsExecutor.shutdown();
        boolean interrupted = Thread.interrupted();
        try {
          if (!requestsExecutor.awaitTermination(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
            LOG.warn("Forcibly shutting down batch executor thread pool.");
            forceShutdownNow(requestsExecutor);
          }
        } catch (InterruptedException e) {
          interrupted = true;
          LOG.debug(
              "Interrupted awaiting termination: forcibly shutting down batch executor thread"
                  + " pool.",
              e);
          forceShutdownNow(requestsExecutor);
        } finally {
          clearFailuresOnClose();
          synchronized (futuresLock) {
            shutdownCompleted = true;
            futuresLock.notifyAll();
          }
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  @VisibleForTesting
  static void forceShutdownNow(ExecutorService executor) {
    for (Runnable droppedTask : executor.shutdownNow()) {
      if (droppedTask instanceof Future<?>) {
        ((Future<?>) droppedTask).cancel(false);
      }
    }
  }

  /**
   * Awaits until all queued requests in the current phase are completed, draining every pending
   * future before throwing any accumulated failure.
   */
  void awaitRequestsCompletion() throws IOException {
    drainPendingFutures();
    throwIfFailedAndRestoreInterrupt();
  }

  private void drainPendingFutures() throws IOException {
    while (true) {
      Future<Void> future = null;
      InterruptedException waitInterrupted = null;
      synchronized (futuresLock) {
        future = responseFutures.pollFirst();
        if (future != null) {
          activePolledFutures++;
        } else if (activePolledFutures == 0 || closed) {
          return;
        } else {
          try {
            futuresLock.wait();
          } catch (InterruptedException e) {
            waitInterrupted = e;
          }
        }
      }
      if (waitInterrupted != null) {
        throw handleWaitInterruption(waitInterrupted);
      }
      if (future == null) {
        continue;
      }

      try {
        if (!future.isDone()) {
          try {
            future.get();
          } catch (InterruptedException e) {
            throw handleCallerInterruption(future, e);
          } catch (CancellationException | ExecutionException ignored) {
            // Fall through to harvestDoneFuture(future), where future.isDone() is now true.
          }
        }
        harvestDoneFuture(future);
      } finally {
        synchronized (futuresLock) {
          activePolledFutures--;
          if (activePolledFutures == 0) {
            futuresLock.notifyAll();
          }
        }
      }
    }
  }

  private void closeSubmissionGateAndAwaitActiveQueues() throws InterruptedIOException {
    synchronized (futuresLock) {
      shutdownInitiated = true;
    }
    requestsExecutor.shutdown();

    InterruptedException waitInterrupted = null;
    synchronized (futuresLock) {
      while (!closed && activeQueueCalls > 0) {
        try {
          futuresLock.wait();
        } catch (InterruptedException e) {
          waitInterrupted = e;
          break;
        }
      }
    }
    if (waitInterrupted != null) {
      throw handleWaitInterruption(waitInterrupted);
    }
  }

  private void throwIfFailedAndRestoreInterrupt() throws IOException {
    IOException failure = consumeFailureAndRestoreInterruptIfNeeded();
    if (failure != null) {
      throw failure;
    }
  }

  private void closeAndCancelRemainingFutures() {
    List<Future<Void>> remaining;
    synchronized (futuresLock) {
      shutdownInitiated = true;
      closed = true;
      remaining = new ArrayList<>(responseFutures);
      responseFutures.clear();
      futuresLock.notifyAll();
    }
    cancelFuturesAndPurge(remaining);
  }

  private void cancelRemainingFutures() {
    List<Future<Void>> remaining;
    synchronized (futuresLock) {
      remaining = new ArrayList<>(responseFutures);
      responseFutures.clear();
    }
    cancelFuturesAndPurge(remaining);
  }

  private void cancelFuturesAndPurge(List<Future<Void>> futures) {
    for (Future<Void> remainingFuture : futures) {
      remainingFuture.cancel(true);
    }
    if (requestsExecutor instanceof ThreadPoolExecutor) {
      ((ThreadPoolExecutor) requestsExecutor).purge();
    }
  }

  private InterruptedIOException handleCallerInterruption(
      Future<Void> currentFuture, InterruptedException e) {
    currentFuture.cancel(true);
    return handleWaitInterruption(e);
  }

  private InterruptedIOException handleWaitInterruption(InterruptedException e) {
    cancelRemainingFutures();
    IOException priorFailure = consumeFailureAndRestoreInterruptIfNeeded();
    Thread.currentThread().interrupt();
    InterruptedIOException interruptedException =
        new InterruptedIOException("Interrupted while awaiting batch task completion");
    interruptedException.initCause(e);
    if (priorFailure != null) {
      interruptedException.addSuppressed(priorFailure);
    }
    return interruptedException;
  }

  private void setRestoreInterruptOnAwait() {
    synchronized (failureLock) {
      if (acceptFailures) {
        restoreInterruptOnAwait = true;
      }
    }
  }

  private void recordFailure(IOException failure, boolean restoreInterrupt) {
    synchronized (failureLock) {
      if (!acceptFailures) {
        return;
      }
      if (restoreInterrupt) {
        restoreInterruptOnAwait = true;
      }
      if (primaryFailure == null) {
        primaryFailure = failure;
      } else if (primaryFailure != failure && suppressedCount < MAX_SUPPRESSED) {
        if (seenFailures == null) {
          seenFailures = Collections.newSetFromMap(new IdentityHashMap<>());
          seenFailures.add(primaryFailure);
        }
        if (seenFailures.add(failure)) {
          primaryFailure.addSuppressed(failure);
          suppressedCount++;
        }
      }
    }
  }

  private void clearFailuresOnClose() {
    synchronized (failureLock) {
      acceptFailures = false;
      primaryFailure = null;
      seenFailures = null;
      suppressedCount = 0;
      restoreInterruptOnAwait = false;
    }
  }

  @Nullable
  private IOException consumeFailureAndRestoreInterruptIfNeeded() {
    IOException failure;
    boolean shouldInterrupt;
    synchronized (failureLock) {
      failure = primaryFailure;
      shouldInterrupt = restoreInterruptOnAwait;
      primaryFailure = null;
      seenFailures = null;
      suppressedCount = 0;
      restoreInterruptOnAwait = false;
    }
    if (shouldInterrupt) {
      Thread.currentThread().interrupt();
    }
    return failure;
  }

  /** Retrieves the result of a Future, converting exceptions to IOException. */
  private void getFromFuture(Future<Void> future) throws IOException {
    try {
      if (future.isDone()) {
        Uninterruptibles.getUninterruptibly(future);
      } else {
        future.get();
      }
    } catch (InterruptedException e) {
      throw handleCallerInterruption(future, e);
    } catch (CancellationException e) {
      throw new IOException("Batch task cancelled", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        InterruptedIOException interruptedException =
            new InterruptedIOException("Batch task interrupted");
        interruptedException.initCause(cause);
        throw interruptedException;
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Batch task failed", e);
    }
  }
}
