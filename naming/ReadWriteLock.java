package naming;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A read-write lock implementation that allows multiple readers to access the resource concurrently
 * but only one writer at a time. If a writer is active, no readers are allowed to access the resource.
 * The lock is fair, meaning that the requests are granted in the order they were made.
 */
public class ReadWriteLock {
    /** The control lock to handle multiple lock/unlock requests manage control flow */
    private final ReentrantLock controlLock;
    /** The queue of lock requests for the path */
    private final Queue<LockRequest> queue;
    /** The number of active readers */
    private final AtomicInteger activeReaders;
    /** The flag to indicate if a writer is active */
    private final AtomicBoolean activeWriter;
    /** The total number of readers that have accessed the resource */
    private final AtomicInteger totalReaders;
    /** The flag to indicate if a write operation is in progress */
    private final AtomicBoolean writeOperationInProgress = new AtomicBoolean(false);

    /**
     * Instantiates a new Read write lock.
     */
    public ReadWriteLock() {
        controlLock = new ReentrantLock(true);
        activeReaders = new AtomicInteger(0);
        activeWriter = new AtomicBoolean(false);
        totalReaders = new AtomicInteger(0);
        queue = new ConcurrentLinkedQueue<>();
    }

    /**
     * Lock the resource for reading or writing.
     * It updates the lock state, queue, and active readers/writers accordingly.
     * @param exclusive the exclusive flag to indicate if the lock is for writing or reading
     * @throws InterruptedException the interrupted exception
     */
    public void lock(boolean exclusive) throws InterruptedException {
        controlLock.lock();
        try {
            System.out.println("Thread " + Thread.currentThread() + " is requesting " + (exclusive ? "exclusive" : "shared") + " lock");
            LockRequest request = new LockRequest(exclusive, Thread.currentThread());
            queue.add(request);
            if (exclusive) {
                while (!canGrantExclusiveAccess(request)) {
                    request.condition.await();
                }
                System.out.println("Thread " + Thread.currentThread() + " granted exclusive lock");
                activeWriter.set(true);
                queue.remove(request); // Remove the request upon granting the lock
            } else {
                while (!canGrantSharedAccess(request)) {
                    request.condition.await();
                }
                activeReaders.incrementAndGet();
                totalReaders.incrementAndGet();
                queue.removeIf(r -> r.owner == Thread.currentThread() && !r.exclusive); // Remove all shared requests by this thread
            }
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Unlock the resource for reading or writing.
     * It updates the lock state, queue, and active readers/writers accordingly.
     * @param exclusive the exclusive flag to indicate if the lock is for writing or reading
     */
    public void unlock(boolean exclusive) {
        controlLock.lock();
        try {
            System.out.println("Thread " + Thread.currentThread() + " is releasing " + (exclusive ? "exclusive" : "shared") + " lock");
            if (exclusive) {
                activeWriter.set(false);
                writeOperationInProgress.set(false);
            } else {
                activeReaders.decrementAndGet();
            }

            if (!queue.isEmpty()) {
                if (!activeWriter.get()) {
                    // There's no active writer. Check if the front of the queue is a read request
                    boolean frontIsRead = !queue.peek().exclusive;
                    if (frontIsRead) {
                        // If the front of the queue is a read request, signal all read requests at the front of the queue
                        for (LockRequest request : queue) {
                            if (!request.exclusive) {
                                request.condition.signal(); // Signal all non-exclusive (read) requests
                            } else {
                                break; // Stop at the first exclusive (write) request
                            }
                        }
                    } else if (activeReaders.get() == 0) {
                        // If there are no active readers, signal the next request in line regardless of its type
                        queue.peek().condition.signal();
                    }
                }
            }
            System.out.println("Thread " + Thread.currentThread() + " released " + (exclusive ? "exclusive" : "shared") + " lock");
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Check if the lock can be granted exclusively.
     * The lock can be granted exclusively if the request is the first in the queue, there are no active readers,
     * and there is no active writer.
     * @param request the lock request
     * @return the boolean
     */
    private boolean canGrantExclusiveAccess(LockRequest request) {
        return isRequestFirst(request) && activeReaders.get() == 0 && !activeWriter.get();
    }

    /**
     * Check if the lock can be granted as a shared lock.
     * The lock can be granted as a shared lock if there is no active writer and the request is the first or among the first shared in the queue.
     * @param request the lock request
     * @return the boolean
     */
    private boolean canGrantSharedAccess(LockRequest request) {
        // Can grant if there's no active writer and the request is the first or among the first shared in the queue
        return !activeWriter.get() && (isRequestFirst(request) || queue.stream().takeWhile(r -> !r.exclusive).anyMatch(r -> r.owner == request.owner));
    }

    /**
     * Check if the request is the first in the queue.
     * @param request the lock request
     * @return the boolean
     */
    private boolean isRequestFirst(LockRequest request) {
        return queue.peek().equals(request);
    }

    /**
     * Returns a string representation of the lock state.
     * @return the string representation of the lock state
     */
    @Override
    public String toString() {
        return "activeReaders=" + activeReaders + ", activeWriter=" + activeWriter + ", queue=" + queue;
    }

    /**
     * Gets the total number of readers that have accessed the resource to be used for replication.
     * @return the total number of readers
     */
    public int getTotalReaders() {
        return totalReaders.get();
    }

    /**
     * Checks if a write operation is in progress.
     *
     * @return true if a write operation is in progress, false otherwise.
     */
    public boolean isWriteOperationInProgress() {
        return writeOperationInProgress.get();
    }

    /**
     * Sets the write operation in progress flag.
     *
     * @param inProgress the in progress flag
     */
    public void setWriteOperationInProgress(boolean inProgress) {
        writeOperationInProgress.set(inProgress);
    }

    /**
     * The LockRequest class to encapsulate the lock request details.
     */
    private class LockRequest {
        /** The exclusive flag to indicate if the lock is for writing or reading */
        final boolean exclusive;
        /** The owner thread of the lock request */
        final Thread owner;
        /** The condition to signal the lock request */
        final Condition condition;

        /**
         * Instantiates a new Lock request.
         *
         * @param exclusive the exclusive flag to indicate if the lock is for writing or reading
         * @param owner     the owner thread of the lock request
         */
        LockRequest(boolean exclusive, Thread owner) {
            this.exclusive = exclusive;
            this.owner = owner;
            this.condition = controlLock.newCondition();
        }

        /**
         * Returns a string representation of the lock request.
         * @return the string representation of the lock request
         */
        @Override
        public String toString() {
            return "LockRequest{exclusive=" + exclusive + ", owner=" + owner + "}";
        }
    }
}
