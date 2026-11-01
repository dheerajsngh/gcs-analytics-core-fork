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

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnauthenticatedException;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Thread-safe {@link FutureCallback} that aggregates failures across concurrent batch tasks with
 * bounded memory usage and deterministic primary {@link IOException} selection.
 *
 * <p>Up to {@link #MAX_RETAINED_FAILURES} failure instances are retained in memory (with
 * priority-aware eviction once full so higher-priority exceptions, interrupts, and fatal {@link
 * VirtualMachineError}s are never displaced by lower-priority errors) while {@link
 * #getFailureCount()} tracks the exact total number of non-ignored failures, ensuring large batches
 * (e.g., millions of failing tasks) cannot exhaust heap memory.
 */
final class BatchErrorCollector implements FutureCallback<Object> {

  @VisibleForTesting static final int MAX_RETAINED_FAILURES = 100;

  private static final int PRIORITY_VM_ERROR = 0;
  private static final int PRIORITY_INTERRUPTED = 1;
  private static final int PRIORITY_ACCESS_DENIED = 2;
  private static final int PRIORITY_DIRECTORY_NOT_EMPTY = 3;
  private static final int PRIORITY_FILE_ALREADY_EXISTS = 4;
  private static final int PRIORITY_FILE_NOT_FOUND = 5;
  private static final int PRIORITY_OTHER_IO = 6;

  private final boolean ignoreNotFound;
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final Object retentionLock = new Object();
  private final Object throwLock = new Object();

  /**
   * Monotonically non-increasing worst (numerically largest) priority among the {@link
   * #MAX_RETAINED_FAILURES} retained entries once full, or {@link Integer#MAX_VALUE} before full.
   */
  private volatile int worstRetainedPriority = Integer.MAX_VALUE;

  /**
   * Unmodifiable identity-based lookup set of currently retained raw and unwrapped failures once
   * {@link #retainedFailures} reaches {@link #MAX_RETAINED_FAILURES}, published before {@link
   * #worstRetainedPriority} updates so lock-free readers never miss duplicates.
   */
  @Nullable private volatile Set<Throwable> fullRetainedLookup;

  @GuardedBy("retentionLock")
  private final List<Throwable> retainedFailures = new ArrayList<>();

  @GuardedBy("retentionLock")
  private final int[] retainedPriorities = new int[MAX_RETAINED_FAILURES];

  @GuardedBy("retentionLock")
  private final int[] retainedDuplicateCounts = new int[MAX_RETAINED_FAILURES];

  @GuardedBy("retentionLock")
  private int retentionMutationVersion = 0;

  @GuardedBy("throwLock")
  @Nullable
  private IOException cachedPrimary;

  @GuardedBy("throwLock")
  private int cachedFailureCount = -1;

  @GuardedBy("throwLock")
  private int cachedMutationVersion = -1;

  @GuardedBy("throwLock")
  @Nullable
  private String cachedOperationSummary;

  @GuardedBy("throwLock")
  private final IdentityHashMap<IOException, OmittedFailuresIOException> ownedSummaries =
      new IdentityHashMap<>();

  @GuardedBy("throwLock")
  private final IdentityHashMap<IOException, Set<Throwable>> attachedRawByPrimary =
      new IdentityHashMap<>();

  BatchErrorCollector(boolean ignoreNotFound) {
    this.ignoreNotFound = ignoreNotFound;
  }

  /** Creates a collector that records all failures, including not-found errors. */
  public static BatchErrorCollector create() {
    return new BatchErrorCollector(false);
  }

  /** Creates a collector with configurable {@code ignoreNotFound} behavior. */
  public static BatchErrorCollector create(boolean ignoreNotFound) {
    return new BatchErrorCollector(ignoreNotFound);
  }

  /** Creates a collector that silently ignores not-found errors (HTTP 404 / gRPC NOT_FOUND). */
  public static BatchErrorCollector ignoringNotFound() {
    return new BatchErrorCollector(true);
  }

  /**
   * Lock-free check returning {@code true} if at least one non-ignored failure has been recorded.
   *
   * <p>Streaming page producers can poll this method to stop enqueuing new work early once an error
   * has occurred.
   */
  public boolean hasErrors() {
    return failureCount.get() > 0;
  }

  /** Returns the total number of non-ignored failures recorded so far. */
  public int getFailureCount() {
    return failureCount.get();
  }

  @Override
  public void onSuccess(@Nullable Object result) {
    // No-op: batch collector only records task failures.
  }

  @Override
  public void onFailure(Throwable t) {
    checkNotNull(t, "t should not be null");
    if (ignoreNotFound) {
      if (isNotFound(t)) {
        return;
      }
      if (hasCausalChainNotFound(t)) {
        List<Throwable> extracted = new ArrayList<>(2);
        Set<Throwable> evaluatedRoots = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<Throwable> scannedChainNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        extractNonNotFoundFailures(t, extracted, evaluatedRoots, scannedChainNodes);
        VirtualMachineError vmError = findVirtualMachineError(t);
        if (extracted.isEmpty()) {
          recordSingleFailure(vmError != null ? vmError : t);
        } else {
          if (vmError != null && !containsVirtualMachineError(extracted)) {
            recordSingleFailure(vmError);
          }
          for (int i = 0; i < extracted.size(); i++) {
            recordSingleFailure(extracted.get(i));
          }
        }
        return;
      }
    }
    recordSingleFailure(t);
  }

  private static boolean containsVirtualMachineError(List<Throwable> throwables) {
    for (int i = 0; i < throwables.size(); i++) {
      if (findVirtualMachineError(throwables.get(i)) != null) {
        return true;
      }
    }
    return false;
  }

  private void recordSingleFailure(Throwable t) {
    int worst = worstRetainedPriority;
    if (worst < Integer.MAX_VALUE) {
      int incomingPriority = rawPriorityOf(t);
      if (incomingPriority >= worst) {
        Set<Throwable> lookup = fullRetainedLookup;
        if (lookup != null && !containsSameOrUnwrapped(lookup, t)) {
          failureCount.incrementAndGet();
          return;
        }
      }
      recordFailureUnderLock(t, incomingPriority);
      return;
    }
    recordFailureUnderLock(t, -1);
  }

  private static boolean containsSameOrUnwrapped(Set<Throwable> lookup, Throwable incoming) {
    if (lookup.contains(incoming)) {
      return true;
    }
    Throwable incomingUnwrapped = unwrapTransparentWrappers(incoming);
    return incomingUnwrapped != incoming && lookup.contains(incomingUnwrapped);
  }

  private void recordFailureUnderLock(Throwable t, int precomputedPriority) {
    synchronized (retentionLock) {
      int existingIndex = findMatchingRetainedIndexLocked(t);
      if (existingIndex >= 0) {
        retainedDuplicateCounts[existingIndex]++;
        failureCount.incrementAndGet();
        return;
      }

      int size = retainedFailures.size();
      if (size < MAX_RETAINED_FAILURES) {
        retainedFailures.add(t);
        retainedDuplicateCounts[size] = 0;
        retentionMutationVersion++;
        if (size + 1 == MAX_RETAINED_FAILURES) {
          int worst = PRIORITY_VM_ERROR;
          for (int i = 0; i < MAX_RETAINED_FAILURES; i++) {
            int p =
                (i == MAX_RETAINED_FAILURES - 1 && precomputedPriority >= 0)
                    ? precomputedPriority
                    : rawPriorityOf(retainedFailures.get(i));
            retainedPriorities[i] = p;
            if (p > worst) {
              worst = p;
            }
          }
          fullRetainedLookup = buildLookupLocked();
          worstRetainedPriority = worst;
        }
        failureCount.incrementAndGet();
        return;
      }

      int incomingPriority = precomputedPriority >= 0 ? precomputedPriority : rawPriorityOf(t);
      if (incomingPriority < worstRetainedPriority) {
        int evictIndex = -1;
        for (int i = MAX_RETAINED_FAILURES - 1; i >= 0; i--) {
          if (retainedPriorities[i] == worstRetainedPriority) {
            evictIndex = i;
            break;
          }
        }
        if (evictIndex >= 0) {
          retainedFailures.remove(evictIndex);
          retainedFailures.add(t);
          int moveCount = MAX_RETAINED_FAILURES - 1 - evictIndex;
          if (moveCount > 0) {
            System.arraycopy(
                retainedPriorities, evictIndex + 1, retainedPriorities, evictIndex, moveCount);
            System.arraycopy(
                retainedDuplicateCounts,
                evictIndex + 1,
                retainedDuplicateCounts,
                evictIndex,
                moveCount);
          }
          retainedPriorities[MAX_RETAINED_FAILURES - 1] = incomingPriority;
          retainedDuplicateCounts[MAX_RETAINED_FAILURES - 1] = 0;
          retentionMutationVersion++;

          int worst = PRIORITY_VM_ERROR;
          for (int i = 0; i < MAX_RETAINED_FAILURES; i++) {
            if (retainedPriorities[i] > worst) {
              worst = retainedPriorities[i];
            }
          }
          fullRetainedLookup = buildLookupLocked();
          worstRetainedPriority = worst;
        }
      }
      failureCount.incrementAndGet();
    }
  }

  @GuardedBy("retentionLock")
  private int findMatchingRetainedIndexLocked(Throwable incoming) {
    Throwable incomingUnwrapped = unwrapTransparentWrappers(incoming);
    for (int i = 0; i < retainedFailures.size(); i++) {
      Throwable existing = retainedFailures.get(i);
      if (existing == incoming || existing == incomingUnwrapped) {
        return i;
      }
      Throwable existingUnwrapped = unwrapTransparentWrappers(existing);
      if (existingUnwrapped == incoming || existingUnwrapped == incomingUnwrapped) {
        return i;
      }
    }
    return -1;
  }

  @GuardedBy("retentionLock")
  private Set<Throwable> buildLookupLocked() {
    Set<Throwable> set =
        Collections.newSetFromMap(new IdentityHashMap<>(MAX_RETAINED_FAILURES * 2));
    for (int i = 0; i < retainedFailures.size(); i++) {
      Throwable existing = retainedFailures.get(i);
      set.add(existing);
      set.add(unwrapTransparentWrappers(existing));
    }
    return set;
  }

  /**
   * Returns {@code true} if {@code t}'s causal chain represents a missing object, file, or resource
   * and every suppressed exception attached along that causal chain is also recursively {@code
   * isNotFound}, guarding against cycles via reference identity.
   */
  static boolean isNotFound(@Nullable Throwable t) {
    if (t == null) {
      return false;
    }
    if (t.getCause() == null && t.getSuppressed().length == 0) {
      return Boolean.TRUE.equals(classifyNotFoundNode(t));
    }
    if (findVirtualMachineError(t) != null) {
      return false;
    }
    Set<Throwable> verifiedOrPending = Collections.newSetFromMap(new IdentityHashMap<>());
    Set<Throwable> tailScanned = Collections.newSetFromMap(new IdentityHashMap<>());
    return isNotFoundRecursive(t, verifiedOrPending, tailScanned);
  }

  private static boolean isNotFoundRecursive(
      Throwable root, Set<Throwable> verifiedOrPending, Set<Throwable> tailScanned) {
    if (verifiedOrPending.contains(root)) {
      return true;
    }
    Set<Throwable> chainVisited = Collections.newSetFromMap(new IdentityHashMap<>());
    List<Throwable> verifiedPrefix = new ArrayList<>(4);
    Throwable current = root;
    boolean matchedNotFound = false;
    while (current != null && chainVisited.add(current)) {
      if (verifiedOrPending.contains(current)) {
        matchedNotFound = true;
        break;
      }
      verifiedPrefix.add(current);
      Boolean nodeVerdict = classifyNotFoundNode(current);
      if (nodeVerdict != null) {
        if (!nodeVerdict) {
          return false;
        }
        matchedNotFound = true;
        break;
      }
      current = current.getCause();
    }
    if (!matchedNotFound) {
      return false;
    }

    verifiedOrPending.addAll(verifiedPrefix);

    List<Throwable> tailNodes = null;
    Throwable tail = current != null ? current.getCause() : null;
    while (tail != null && chainVisited.add(tail)) {
      if (!verifiedOrPending.contains(tail) && tailScanned.add(tail)) {
        if (tailNodes == null) {
          tailNodes = new ArrayList<>(2);
        }
        tailNodes.add(tail);
      }
      tail = tail.getCause();
    }

    for (int i = 0; i < verifiedPrefix.size(); i++) {
      for (Throwable suppressed : verifiedPrefix.get(i).getSuppressed()) {
        if (suppressed != null
            && !isNotFoundRecursive(suppressed, verifiedOrPending, tailScanned)) {
          return false;
        }
      }
    }
    if (tailNodes != null) {
      for (int i = 0; i < tailNodes.size(); i++) {
        for (Throwable suppressed : tailNodes.get(i).getSuppressed()) {
          if (suppressed != null
              && !isNotFoundRecursive(suppressed, verifiedOrPending, tailScanned)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private static boolean hasCausalChainNotFound(Throwable t) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = t;
    while (current != null && visited.add(current)) {
      Boolean nodeVerdict = classifyNotFoundNode(current);
      if (nodeVerdict != null) {
        return nodeVerdict;
      }
      current = current.getCause();
    }
    return false;
  }

  private static void extractNonNotFoundFailures(
      Throwable root,
      List<Throwable> out,
      Set<Throwable> evaluatedRoots,
      Set<Throwable> scannedChainNodes) {
    if (!evaluatedRoots.add(root) || isNotFound(root)) {
      return;
    }
    if (!hasCausalChainNotFound(root)) {
      out.add(root);
      return;
    }
    Throwable current = root;
    while (current != null && scannedChainNodes.add(current)) {
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null) {
          extractNonNotFoundFailures(suppressed, out, evaluatedRoots, scannedChainNodes);
        }
      }
      current = current.getCause();
    }
  }

  @Nullable
  private static Boolean classifyNotFoundNode(Throwable current) {
    if (current instanceof VirtualMachineError) {
      return Boolean.FALSE;
    }
    if (current instanceof FileNotFoundException
        || current instanceof NoSuchFileException
        || current instanceof NotFoundException) {
      return Boolean.TRUE;
    }
    if (current instanceof AccessDeniedException
        || current instanceof DirectoryNotEmptyException
        || current instanceof FileAlreadyExistsException
        || current instanceof InterruptedException
        || isInterruptException(current)) {
      return Boolean.FALSE;
    }
    if (current instanceof StorageException) {
      StorageException se = (StorageException) current;
      Throwable cause = se.getCause() != null ? peekTransparentWrappers(se.getCause()) : null;
      if (cause instanceof ApiException) {
        return classifyApiExceptionNotFound((ApiException) cause);
      }
      if (cause instanceof StatusRuntimeException) {
        return isGrpcStatusNotFound(((StatusRuntimeException) cause).getStatus());
      }
      if (cause instanceof StatusException) {
        return isGrpcStatusNotFound(((StatusException) cause).getStatus());
      }
      int code = se.getCode();
      if (code == HttpURLConnection.HTTP_NOT_FOUND) {
        return Boolean.TRUE;
      }
      return code == 0 ? null : Boolean.FALSE;
    }
    if (current instanceof ApiException) {
      return classifyApiExceptionNotFound((ApiException) current);
    }
    if (current instanceof StatusRuntimeException) {
      return isGrpcStatusNotFound(((StatusRuntimeException) current).getStatus());
    }
    if (current instanceof StatusException) {
      return isGrpcStatusNotFound(((StatusException) current).getStatus());
    }
    return null;
  }

  @Nullable
  private static Boolean classifyApiExceptionNotFound(ApiException ae) {
    if (ae instanceof NotFoundException) {
      return Boolean.TRUE;
    }
    if (ae instanceof PermissionDeniedException
        || ae instanceof UnauthenticatedException
        || ae instanceof AlreadyExistsException) {
      return Boolean.FALSE;
    }
    StatusCode statusCode = ae.getStatusCode();
    if (statusCode != null && statusCode.getCode() != null) {
      StatusCode.Code code = statusCode.getCode();
      if (code == StatusCode.Code.NOT_FOUND) {
        return Boolean.TRUE;
      }
      if (code == StatusCode.Code.UNKNOWN) {
        return null;
      }
      return Boolean.FALSE;
    }
    return null;
  }

  @Nullable
  private static Boolean isGrpcStatusNotFound(@Nullable Status status) {
    if (status == null || status.getCode() == null) {
      return null;
    }
    Status.Code code = status.getCode();
    if (code == Status.Code.NOT_FOUND) {
      return Boolean.TRUE;
    }
    if (code == Status.Code.UNKNOWN) {
      return null;
    }
    return Boolean.FALSE;
  }

  /**
   * Throws a composite {@link IOException} if any non-ignored failure was recorded; returns
   * immediately if {@link #getFailureCount()} is zero.
   *
   * <p>Fatal {@link VirtualMachineError} instances (such as {@link OutOfMemoryError}) are rethrown
   * directly without wrapping. All other retained non-{@link IOException} throwables (including
   * {@link RuntimeException} and non-VM {@link Error} subclasses) are converted or wrapped into
   * {@link IOException} instances. The primary {@link IOException} is selected by deterministic
   * priority:
   *
   * <ol>
   *   <li>{@link InterruptedIOException} (excluding {@link SocketTimeoutException}) and {@link
   *       ClosedByInterruptException} (also restores the calling thread's interrupt status)
   *   <li>{@link AccessDeniedException}
   *   <li>{@link DirectoryNotEmptyException}
   *   <li>{@link FileAlreadyExistsException}
   *   <li>{@link FileNotFoundException} (and {@link NoSuchFileException})
   *   <li>Any other {@link IOException}
   * </ol>
   *
   * <p>Remaining retained exceptions are attached to the primary exception via {@link
   * Throwable#addSuppressed(Throwable)} while guarding against self-suppression, duplicate object
   * references, and causal cycles. If {@link #getFailureCount()} exceeds the number of retained
   * failures, a final synthetic {@link IOException} summarizing the omitted count is appended.
   *
   * @param operationSummary human-readable summary of the batch operation used when wrapping
   *     non-I/O failures
   * @throws IOException the highest-priority failure with secondary failures attached as suppressed
   */
  public void throwIfFailed(String operationSummary) throws IOException {
    checkNotNull(operationSummary, "operationSummary should not be null");
    if (failureCount.get() == 0) {
      return;
    }

    IOException primaryToThrow = resolvePrimaryException(operationSummary);
    if (isInterruptException(primaryToThrow)) {
      Thread.currentThread().interrupt();
    }
    throw primaryToThrow;
  }

  private IOException resolvePrimaryException(String operationSummary) {
    synchronized (throwLock) {
      int totalFailures;
      int mutationVersion;
      int retainedDuplicates = 0;
      List<Throwable> snapshot;
      synchronized (retentionLock) {
        totalFailures = failureCount.get();
        mutationVersion = retentionMutationVersion;
        if (cachedPrimary != null
            && cachedFailureCount == totalFailures
            && cachedMutationVersion == mutationVersion
            && operationSummary.equals(cachedOperationSummary)) {
          return cachedPrimary;
        }
        int size = retainedFailures.size();
        snapshot = new ArrayList<>(retainedFailures);
        for (int i = 0; i < size; i++) {
          retainedDuplicates += retainedDuplicateCounts[i];
        }
      }

      IOException built =
          buildCompositeException(snapshot, retainedDuplicates, totalFailures, operationSummary);
      cachedPrimary = built;
      cachedFailureCount = totalFailures;
      cachedMutationVersion = mutationVersion;
      cachedOperationSummary = operationSummary;
      return built;
    }
  }

  private static final class ConvertedFailure {
    final IOException converted;
    final Throwable raw;
    final Throwable unwrapped;

    ConvertedFailure(IOException converted, Throwable raw, Throwable unwrapped) {
      this.converted = converted;
      this.raw = raw;
      this.unwrapped = unwrapped;
    }
  }

  private static final class AttachableFailure {
    final IOException converted;
    final Throwable rawSource;

    AttachableFailure(IOException converted, Throwable rawSource) {
      this.converted = converted;
      this.rawSource = rawSource;
    }
  }

  private static final class OmittedFailuresIOException extends IOException {
    private static final long serialVersionUID = 1L;
    private volatile int omittedCount;

    OmittedFailuresIOException(int omittedCount) {
      super((String) null);
      this.omittedCount = omittedCount;
    }

    void setOmittedCount(int omittedCount) {
      this.omittedCount = omittedCount;
    }

    @Override
    public String getMessage() {
      return "... and " + omittedCount + " more suppressed failures";
    }
  }

  @GuardedBy("throwLock")
  private IOException buildCompositeException(
      List<Throwable> snapshot,
      int retainedDuplicates,
      int totalFailures,
      String operationSummary) {
    List<ConvertedFailure> candidates = new ArrayList<>(snapshot.size());
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable raw : snapshot) {
      Throwable unwrapped = unwrapTransparentWrappers(raw);
      boolean isNewRaw = seen.add(raw);
      boolean isNewUnwrapped = (unwrapped == raw) ? isNewRaw : seen.add(unwrapped);
      if (isNewRaw && isNewUnwrapped) {
        candidates.add(new ConvertedFailure(toIOException(raw, operationSummary), raw, unwrapped));
      }
    }

    ConvertedFailure primaryCandidate = selectPrimary(candidates);
    attachSuppressedExceptions(
        primaryCandidate,
        candidates,
        snapshot,
        retainedDuplicates,
        totalFailures,
        operationSummary);
    return primaryCandidate.converted;
  }

  @GuardedBy("throwLock")
  private void attachSuppressedExceptions(
      ConvertedFailure primaryCandidate,
      List<ConvertedFailure> candidates,
      List<Throwable> snapshot,
      int retainedDuplicates,
      int totalFailures,
      String operationSummary) {
    IOException primary = primaryCandidate.converted;
    Set<Throwable> recordedOnPrimary =
        attachedRawByPrimary.computeIfAbsent(
            primary, k -> Collections.newSetFromMap(new IdentityHashMap<>()));
    recordedOnPrimary.add(primaryCandidate.raw);

    Set<Throwable> attached = Collections.newSetFromMap(new IdentityHashMap<>());
    collectReachable(primary, attached);
    attached.add(primaryCandidate.raw);
    attached.add(primaryCandidate.unwrapped);

    List<AttachableFailure> attachableOthers = new ArrayList<>(candidates.size());
    for (ConvertedFailure candidate : candidates) {
      IOException other = candidate.converted;
      if (primary == other
          || attached.contains(other)
          || attached.contains(candidate.raw)
          || attached.contains(candidate.unwrapped)) {
        continue;
      }
      if (!canReach(other, primary)) {
        attachableOthers.add(new AttachableFailure(other, candidate.raw));
        collectReachable(other, attached);
        attached.add(candidate.raw);
        attached.add(candidate.unwrapped);
      } else {
        Set<Throwable> wrapperVisited = Collections.newSetFromMap(new IdentityHashMap<>());
        collectNonCyclicWrapperSuppressed(
            candidate.raw,
            candidate.raw,
            primary,
            operationSummary,
            attached,
            wrapperVisited,
            attachableOthers);
      }
    }

    Set<Throwable> snapshotRawSet = Collections.newSetFromMap(new IdentityHashMap<>());
    snapshotRawSet.addAll(snapshot);
    int evictedButAlreadyAttached = 0;
    for (Throwable prevRaw : recordedOnPrimary) {
      if (!snapshotRawSet.contains(prevRaw)) {
        evictedButAlreadyAttached++;
      }
    }

    int baseOmitted =
        Math.max(
            0, totalFailures - snapshot.size() - retainedDuplicates - evictedButAlreadyAttached);
    OmittedFailuresIOException existingSummary = ownedSummaries.get(primary);
    int existingSuppressedCount =
        primary.getSuppressed().length - (existingSummary != null ? 1 : 0);
    boolean needsSummary =
        existingSummary != null
            || baseOmitted > 0
            || existingSuppressedCount + attachableOthers.size() > MAX_RETAINED_FAILURES;
    int maxOtherCount;
    if (existingSummary != null) {
      maxOtherCount = 0;
    } else if (needsSummary) {
      maxOtherCount = Math.max(0, (MAX_RETAINED_FAILURES - 1) - existingSuppressedCount);
    } else {
      maxOtherCount = Math.max(0, MAX_RETAINED_FAILURES - existingSuppressedCount);
    }

    int toAttach = Math.min(attachableOthers.size(), maxOtherCount);
    for (int i = 0; i < toAttach; i++) {
      AttachableFailure item = attachableOthers.get(i);
      primary.addSuppressed(item.converted);
      recordedOnPrimary.add(item.rawSource);
    }

    int unattachedCount = countUnattachedSources(attachableOthers, toAttach, recordedOnPrimary);
    int omitted = baseOmitted + unattachedCount;
    if (existingSummary != null) {
      existingSummary.setOmittedCount(omitted);
    } else if (omitted > 0 && primary.getSuppressed().length <= MAX_RETAINED_FAILURES) {
      OmittedFailuresIOException summary = new OmittedFailuresIOException(omitted);
      primary.addSuppressed(summary);
      ownedSummaries.put(primary, summary);
    }
  }

  private static void collectNonCyclicWrapperSuppressed(
      Throwable wrapperRoot,
      Throwable rawSource,
      IOException primary,
      String operationSummary,
      Set<Throwable> attached,
      Set<Throwable> visited,
      List<AttachableFailure> attachableOthers) {
    Throwable current = wrapperRoot;
    while (current != null && current != primary && visited.add(current)) {
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed == null || attached.contains(suppressed)) {
          continue;
        }
        Throwable unwrappedSuppressed = unwrapTransparentWrappers(suppressed);
        if (attached.contains(unwrappedSuppressed)) {
          continue;
        }
        IOException converted = toIOException(suppressed, operationSummary);
        if (primary == converted || attached.contains(converted)) {
          continue;
        }
        if (!canReach(converted, primary)) {
          attachableOthers.add(new AttachableFailure(converted, rawSource));
          collectReachable(converted, attached);
          attached.add(suppressed);
          attached.add(unwrappedSuppressed);
        } else {
          collectNonCyclicWrapperSuppressed(
              suppressed,
              rawSource,
              primary,
              operationSummary,
              attached,
              visited,
              attachableOthers);
        }
      }
      current = current.getCause();
    }
  }

  private static int countUnattachedSources(
      List<AttachableFailure> attachableOthers, int toAttach, Set<Throwable> recordedOnPrimary) {
    if (toAttach >= attachableOthers.size()) {
      return 0;
    }
    Set<Throwable> unattachedRawSources = Collections.newSetFromMap(new IdentityHashMap<>());
    for (int i = toAttach; i < attachableOthers.size(); i++) {
      Throwable rawSource = attachableOthers.get(i).rawSource;
      if (!recordedOnPrimary.contains(rawSource)) {
        unattachedRawSources.add(rawSource);
      }
    }
    return unattachedRawSources.size();
  }

  private static ConvertedFailure selectPrimary(List<ConvertedFailure> candidates) {
    ConvertedFailure best = candidates.get(0);
    int bestPriority = priorityOf(best.converted);
    for (int i = 1; i < candidates.size(); i++) {
      ConvertedFailure candidate = candidates.get(i);
      int candidatePriority = priorityOf(candidate.converted);
      if (candidatePriority < bestPriority) {
        best = candidate;
        bestPriority = candidatePriority;
        if (bestPriority == PRIORITY_INTERRUPTED) {
          break;
        }
      }
    }
    return best;
  }

  private static int priorityOf(IOException e) {
    if (isInterruptException(e)) {
      return PRIORITY_INTERRUPTED;
    }
    if (e instanceof AccessDeniedException) {
      return PRIORITY_ACCESS_DENIED;
    }
    if (e instanceof DirectoryNotEmptyException) {
      return PRIORITY_DIRECTORY_NOT_EMPTY;
    }
    if (e instanceof FileAlreadyExistsException) {
      return PRIORITY_FILE_ALREADY_EXISTS;
    }
    if (e instanceof FileNotFoundException || e instanceof NoSuchFileException) {
      return PRIORITY_FILE_NOT_FOUND;
    }
    return PRIORITY_OTHER_IO;
  }

  private static int rawPriorityOf(Throwable original) {
    if (findVirtualMachineError(original) != null) {
      return PRIORITY_VM_ERROR;
    }
    Throwable candidate =
        (original instanceof IOException) ? original : unwrapTransparentWrappers(original);
    if (candidate instanceof IOException) {
      IOException ioe = (IOException) candidate;
      if (ioe.getClass() == IOException.class && hasUnderlyingInterrupt(ioe)) {
        return PRIORITY_INTERRUPTED;
      }
      return priorityOf(ioe);
    }

    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = candidate;
    while (current != null && visited.add(current)) {
      int classifiedPriority = classifyCausePriority(current);
      if (classifiedPriority >= 0) {
        return classifiedPriority;
      }
      current = current.getCause();
    }
    return PRIORITY_OTHER_IO;
  }

  private static int classifyCausePriority(Throwable current) {
    if (current instanceof VirtualMachineError) {
      return PRIORITY_VM_ERROR;
    }
    if (current instanceof InterruptedException || isInterruptException(current)) {
      return PRIORITY_INTERRUPTED;
    }
    if (current instanceof SocketTimeoutException) {
      return PRIORITY_OTHER_IO;
    }
    if (current instanceof AccessDeniedException) {
      return PRIORITY_ACCESS_DENIED;
    }
    if (current instanceof DirectoryNotEmptyException) {
      return PRIORITY_DIRECTORY_NOT_EMPTY;
    }
    if (current instanceof FileAlreadyExistsException) {
      return PRIORITY_FILE_ALREADY_EXISTS;
    }
    if (current instanceof FileNotFoundException || current instanceof NoSuchFileException) {
      return PRIORITY_FILE_NOT_FOUND;
    }
    if (current instanceof StorageException) {
      return classifyStorageExceptionPriority((StorageException) current);
    }
    if (current instanceof ApiException) {
      return classifyApiExceptionPriority((ApiException) current);
    }
    if (current instanceof StatusRuntimeException) {
      return classifyGrpcStatusPriority(((StatusRuntimeException) current).getStatus());
    }
    if (current instanceof StatusException) {
      return classifyGrpcStatusPriority(((StatusException) current).getStatus());
    }
    return -1;
  }

  private static int classifyStorageExceptionPriority(StorageException se) {
    Throwable cause = se.getCause() != null ? peekTransparentWrappers(se.getCause()) : null;
    if (cause instanceof ApiException) {
      return classifyApiExceptionPriority((ApiException) cause);
    }
    if (cause instanceof StatusRuntimeException) {
      return classifyGrpcStatusPriority(((StatusRuntimeException) cause).getStatus());
    }
    if (cause instanceof StatusException) {
      return classifyGrpcStatusPriority(((StatusException) cause).getStatus());
    }
    switch (se.getCode()) {
      case HttpURLConnection.HTTP_FORBIDDEN:
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        return PRIORITY_ACCESS_DENIED;
      case HttpURLConnection.HTTP_NOT_FOUND:
        return PRIORITY_FILE_NOT_FOUND;
      case HttpURLConnection.HTTP_CONFLICT:
        return PRIORITY_FILE_ALREADY_EXISTS;
      case 0:
        return -1;
      default:
        return PRIORITY_OTHER_IO;
    }
  }

  private static int classifyApiExceptionPriority(ApiException ae) {
    if (ae instanceof PermissionDeniedException || ae instanceof UnauthenticatedException) {
      return PRIORITY_ACCESS_DENIED;
    }
    if (ae instanceof NotFoundException) {
      return PRIORITY_FILE_NOT_FOUND;
    }
    if (ae instanceof AlreadyExistsException) {
      return PRIORITY_FILE_ALREADY_EXISTS;
    }
    StatusCode statusCode = ae.getStatusCode();
    if (statusCode != null && statusCode.getCode() != null) {
      switch (statusCode.getCode()) {
        case PERMISSION_DENIED:
        case UNAUTHENTICATED:
          return PRIORITY_ACCESS_DENIED;
        case NOT_FOUND:
          return PRIORITY_FILE_NOT_FOUND;
        case ALREADY_EXISTS:
          return PRIORITY_FILE_ALREADY_EXISTS;
        case CANCELLED:
        case UNKNOWN:
          return -1;
        default:
          return PRIORITY_OTHER_IO;
      }
    }
    return -1;
  }

  private static int classifyGrpcStatusPriority(@Nullable Status status) {
    if (status == null || status.getCode() == null) {
      return -1;
    }
    switch (status.getCode()) {
      case PERMISSION_DENIED:
      case UNAUTHENTICATED:
        return PRIORITY_ACCESS_DENIED;
      case NOT_FOUND:
        return PRIORITY_FILE_NOT_FOUND;
      case ALREADY_EXISTS:
        return PRIORITY_FILE_ALREADY_EXISTS;
      case CANCELLED:
      case UNKNOWN:
        return -1;
      default:
        return PRIORITY_OTHER_IO;
    }
  }

  private static boolean isInterruptException(Throwable t) {
    return (t instanceof InterruptedIOException && !(t instanceof SocketTimeoutException))
        || t instanceof ClosedByInterruptException;
  }

  private static IOException toIOException(Throwable original, String operationSummary) {
    VirtualMachineError vmError = findVirtualMachineError(original);
    if (vmError != null) {
      throw vmError;
    }

    Throwable candidate =
        (original instanceof IOException) ? original : unwrapTransparentWrappers(original);
    if (candidate instanceof IOException) {
      IOException ioe = (IOException) candidate;
      if (ioe.getClass() == IOException.class && hasUnderlyingInterrupt(ioe)) {
        return initCause(
            new InterruptedIOException(formatMessage(operationSummary, ioe.getMessage())),
            original);
      }
      return ioe;
    }

    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = candidate;
    while (current != null && visited.add(current)) {
      IOException classified = classifyCause(current, original, operationSummary);
      if (classified != null) {
        return classified;
      }
      current = current.getCause();
    }
    return new IOException(operationSummary, original);
  }

  @Nullable
  private static VirtualMachineError findVirtualMachineError(Throwable root) {
    if (root instanceof VirtualMachineError) {
      return (VirtualMachineError) root;
    }
    Throwable directCause = root.getCause();
    Throwable[] directSuppressed = root.getSuppressed();
    if (directCause == null && directSuppressed.length == 0) {
      return null;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    ArrayDeque<Throwable> stack = new ArrayDeque<>();
    visited.add(root);
    for (int i = directSuppressed.length - 1; i >= 0; i--) {
      Throwable suppressed = directSuppressed[i];
      if (suppressed != null && visited.add(suppressed)) {
        stack.push(suppressed);
      }
    }
    if (directCause != null && visited.add(directCause)) {
      stack.push(directCause);
    }
    while (!stack.isEmpty()) {
      Throwable current = stack.pop();
      if (current instanceof VirtualMachineError) {
        return (VirtualMachineError) current;
      }
      Throwable[] suppressed = current.getSuppressed();
      for (int i = suppressed.length - 1; i >= 0; i--) {
        Throwable s = suppressed[i];
        if (s != null && visited.add(s)) {
          stack.push(s);
        }
      }
      Throwable cause = current.getCause();
      if (cause != null && visited.add(cause)) {
        stack.push(cause);
      }
    }
    return null;
  }

  private static boolean hasUnderlyingInterrupt(IOException plainIoe) {
    Throwable current = plainIoe.getCause();
    if (current == null) {
      return false;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    visited.add(plainIoe);
    while (current != null && visited.add(current)) {
      if (current instanceof InterruptedException || isInterruptException(current)) {
        return true;
      }
      if (current instanceof SocketTimeoutException
          || current instanceof AccessDeniedException
          || current instanceof DirectoryNotEmptyException
          || current instanceof FileAlreadyExistsException
          || current instanceof FileNotFoundException
          || current instanceof NoSuchFileException) {
        return false;
      }
      if (current instanceof StorageException) {
        if (!isPassThroughStorageException((StorageException) current)) {
          return false;
        }
      } else if (current instanceof ApiException) {
        if (!isPassThroughApiException((ApiException) current)) {
          return false;
        }
      } else if (current instanceof StatusRuntimeException) {
        if (!isPassThroughGrpcStatus(((StatusRuntimeException) current).getStatus())) {
          return false;
        }
      } else if (current instanceof StatusException) {
        if (!isPassThroughGrpcStatus(((StatusException) current).getStatus())) {
          return false;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean isPassThroughStorageException(StorageException se) {
    Throwable cause = se.getCause() != null ? peekTransparentWrappers(se.getCause()) : null;
    if (cause instanceof ApiException) {
      return isPassThroughApiException((ApiException) cause);
    }
    if (cause instanceof StatusRuntimeException) {
      return isPassThroughGrpcStatus(((StatusRuntimeException) cause).getStatus());
    }
    if (cause instanceof StatusException) {
      return isPassThroughGrpcStatus(((StatusException) cause).getStatus());
    }
    return se.getCode() == 0;
  }

  private static boolean isPassThroughApiException(ApiException ae) {
    if (ae instanceof PermissionDeniedException
        || ae instanceof UnauthenticatedException
        || ae instanceof NotFoundException
        || ae instanceof AlreadyExistsException) {
      return false;
    }
    StatusCode statusCode = ae.getStatusCode();
    if (statusCode == null || statusCode.getCode() == null) {
      return true;
    }
    StatusCode.Code code = statusCode.getCode();
    return code == StatusCode.Code.CANCELLED || code == StatusCode.Code.UNKNOWN;
  }

  private static boolean isPassThroughGrpcStatus(@Nullable Status status) {
    if (status == null || status.getCode() == null) {
      return true;
    }
    Status.Code code = status.getCode();
    return code == Status.Code.CANCELLED || code == Status.Code.UNKNOWN;
  }

  @Nullable
  private static IOException classifyCause(
      Throwable current, Throwable original, String operationSummary) {
    if (current instanceof VirtualMachineError) {
      throw (VirtualMachineError) current;
    }
    if (current instanceof InterruptedException || isInterruptException(current)) {
      return initCause(
          new InterruptedIOException(formatMessage(operationSummary, current.getMessage())),
          original);
    }
    if (current instanceof SocketTimeoutException) {
      return new IOException(formatMessage(operationSummary, current.getMessage()), original);
    }
    if (current instanceof AccessDeniedException) {
      AccessDeniedException ade = (AccessDeniedException) current;
      return initCause(
          new AccessDeniedException(
              ade.getFile(), ade.getOtherFile(), formatMessage(operationSummary, ade.getReason())),
          original);
    }
    if (current instanceof DirectoryNotEmptyException) {
      DirectoryNotEmptyException dnee = (DirectoryNotEmptyException) current;
      return initCause(new DirectoryNotEmptyException(dnee.getFile()), original);
    }
    if (current instanceof FileAlreadyExistsException) {
      FileAlreadyExistsException faee = (FileAlreadyExistsException) current;
      return initCause(
          new FileAlreadyExistsException(
              faee.getFile(),
              faee.getOtherFile(),
              formatMessage(operationSummary, faee.getReason())),
          original);
    }
    if (current instanceof FileNotFoundException || current instanceof NoSuchFileException) {
      return initCause(
          new FileNotFoundException(formatMessage(operationSummary, current.getMessage())),
          original);
    }
    if (current instanceof StorageException) {
      return mapStorageException((StorageException) current, original, operationSummary);
    }
    if (current instanceof ApiException) {
      ApiException ae = (ApiException) current;
      return mapApiException(ae, ae.getMessage(), original, operationSummary);
    }
    if (current instanceof StatusRuntimeException) {
      return mapGrpcStatus(
          ((StatusRuntimeException) current).getStatus(),
          current.getMessage(),
          original,
          operationSummary);
    }
    if (current instanceof StatusException) {
      return mapGrpcStatus(
          ((StatusException) current).getStatus(),
          current.getMessage(),
          original,
          operationSummary);
    }
    return null;
  }

  private static Throwable unwrapTransparentWrappers(Throwable t) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = t;
    while (isTransparentWrapper(current)
        && current.getSuppressed().length == 0
        && current.getCause() != null
        && visited.add(current)) {
      current = current.getCause();
    }
    return current;
  }

  private static Throwable peekTransparentWrappers(Throwable t) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable current = t;
    while (isTransparentWrapper(current) && current.getCause() != null && visited.add(current)) {
      current = current.getCause();
    }
    return current;
  }

  private static boolean isTransparentWrapper(Throwable t) {
    return t instanceof UncheckedIOException
        || t instanceof ExecutionException
        || t instanceof CompletionException
        || t instanceof UncheckedExecutionException
        || t instanceof ExecutionError;
  }

  @Nullable
  private static IOException mapStorageException(
      StorageException se, Throwable original, String operationSummary) {
    Throwable cause = se.getCause() != null ? peekTransparentWrappers(se.getCause()) : null;
    String detailMessage =
        firstNonEmpty(se.getMessage(), cause != null ? cause.getMessage() : null);
    if (cause instanceof ApiException) {
      return mapApiException((ApiException) cause, detailMessage, original, operationSummary);
    }
    if (cause instanceof StatusRuntimeException) {
      return mapGrpcStatus(
          ((StatusRuntimeException) cause).getStatus(), detailMessage, original, operationSummary);
    }
    if (cause instanceof StatusException) {
      return mapGrpcStatus(
          ((StatusException) cause).getStatus(), detailMessage, original, operationSummary);
    }
    switch (se.getCode()) {
      case HttpURLConnection.HTTP_FORBIDDEN:
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        return initCause(
            new AccessDeniedException(null, null, formatMessage(operationSummary, se.getMessage())),
            original);
      case HttpURLConnection.HTTP_NOT_FOUND:
        return initCause(
            new FileNotFoundException(formatMessage(operationSummary, se.getMessage())), original);
      case HttpURLConnection.HTTP_CONFLICT:
        return initCause(
            new FileAlreadyExistsException(
                null, null, formatMessage(operationSummary, se.getMessage())),
            original);
      case 0:
        return null;
      default:
        return new IOException(operationSummary, original);
    }
  }

  @Nullable
  private static IOException mapApiException(
      ApiException ae,
      @Nullable String detailMessage,
      Throwable original,
      String operationSummary) {
    if (ae instanceof PermissionDeniedException || ae instanceof UnauthenticatedException) {
      return initCause(
          new AccessDeniedException(null, null, formatMessage(operationSummary, detailMessage)),
          original);
    }
    if (ae instanceof NotFoundException) {
      return initCause(
          new FileNotFoundException(formatMessage(operationSummary, detailMessage)), original);
    }
    if (ae instanceof AlreadyExistsException) {
      return initCause(
          new FileAlreadyExistsException(
              null, null, formatMessage(operationSummary, detailMessage)),
          original);
    }
    StatusCode statusCode = ae.getStatusCode();
    if (statusCode != null && statusCode.getCode() != null) {
      switch (statusCode.getCode()) {
        case PERMISSION_DENIED:
        case UNAUTHENTICATED:
          return initCause(
              new AccessDeniedException(null, null, formatMessage(operationSummary, detailMessage)),
              original);
        case NOT_FOUND:
          return initCause(
              new FileNotFoundException(formatMessage(operationSummary, detailMessage)), original);
        case ALREADY_EXISTS:
          return initCause(
              new FileAlreadyExistsException(
                  null, null, formatMessage(operationSummary, detailMessage)),
              original);
        case CANCELLED:
        case UNKNOWN:
          return null;
        default:
          return new IOException(operationSummary, original);
      }
    }
    return null;
  }

  @Nullable
  private static IOException mapGrpcStatus(
      @Nullable Status status,
      @Nullable String message,
      Throwable original,
      String operationSummary) {
    if (status == null || status.getCode() == null) {
      return null;
    }
    switch (status.getCode()) {
      case PERMISSION_DENIED:
      case UNAUTHENTICATED:
        return initCause(
            new AccessDeniedException(null, null, formatMessage(operationSummary, message)),
            original);
      case NOT_FOUND:
        return initCause(
            new FileNotFoundException(formatMessage(operationSummary, message)), original);
      case ALREADY_EXISTS:
        return initCause(
            new FileAlreadyExistsException(null, null, formatMessage(operationSummary, message)),
            original);
      case CANCELLED:
      case UNKNOWN:
        return null;
      default:
        return new IOException(operationSummary, original);
    }
  }

  private static <E extends IOException> E initCause(E exception, Throwable cause) {
    exception.initCause(cause);
    return exception;
  }

  @Nullable
  private static String firstNonEmpty(@Nullable String primary, @Nullable String fallback) {
    if (primary != null && !primary.isEmpty()) {
      return primary;
    }
    if (fallback != null && !fallback.isEmpty()) {
      return fallback;
    }
    return null;
  }

  private static String formatMessage(String operationSummary, @Nullable String detailMessage) {
    if (detailMessage == null || detailMessage.isEmpty()) {
      return operationSummary;
    }
    if (operationSummary.isEmpty()) {
      return detailMessage;
    }
    return operationSummary + ": " + detailMessage;
  }

  private static void collectReachable(Throwable root, Set<Throwable> visited) {
    ArrayDeque<Throwable> stack = new ArrayDeque<>();
    if (visited.add(root)) {
      stack.push(root);
    }
    while (!stack.isEmpty()) {
      Throwable current = stack.pop();
      Throwable cause = current.getCause();
      if (cause != null && visited.add(cause)) {
        stack.push(cause);
      }
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null && visited.add(suppressed)) {
          stack.push(suppressed);
        }
      }
    }
  }

  private static boolean canReach(Throwable start, Throwable target) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    ArrayDeque<Throwable> stack = new ArrayDeque<>();
    visited.add(start);
    stack.push(start);
    while (!stack.isEmpty()) {
      Throwable current = stack.pop();
      if (current == target) {
        return true;
      }
      Throwable cause = current.getCause();
      if (cause != null && visited.add(cause)) {
        stack.push(cause);
      }
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed != null && visited.add(suppressed)) {
          stack.push(suppressed);
        }
      }
    }
    return false;
  }
}
