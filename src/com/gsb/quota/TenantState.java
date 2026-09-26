package com.gsb.quota;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-tenant scheduling state: FIFO wait queue, token bucket and the
 * weighted-fair-queuing virtual finish tag of the last enqueued task.
 */
final class TenantState {

    final String id;
    final int weight;
    final TokenBucket bucket;
    final AtomicLong completed = new AtomicLong();

    private final ConcurrentLinkedQueue<QueuedTask> queue = new ConcurrentLinkedQueue<QueuedTask>();
    private final Object tagLock = new Object();
    private double lastTag;

    TenantState(String id, long tokensPerSecond, int weight, Clock clock) {
        this.id = id;
        this.weight = weight;
        this.bucket = new TokenBucket(tokensPerSecond, clock);
    }

    /**
     * Assigns the virtual finish tag for a newly submitted task. Only guarded
     * by a tenant-local lock, so submissions from different tenants never
     * serialize on a shared lock.
     */
    double nextTag(double virtualTime) {
        synchronized (tagLock) {
            double base = Math.max(lastTag, virtualTime);
            lastTag = base + 1.0 / weight;
            return lastTag;
        }
    }

    void enqueue(QueuedTask task) {
        queue.add(task);
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    double headTag() {
        QueuedTask head = queue.peek();
        return head == null ? Double.MAX_VALUE : head.tag;
    }

    QueuedTask poll() {
        return queue.poll();
    }

    long queueSize() {
        return queue.size();
    }
}
