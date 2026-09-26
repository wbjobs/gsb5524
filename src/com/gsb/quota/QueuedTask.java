package com.gsb.quota;

import java.util.concurrent.Callable;

/**
 * A submitted task plus its weighted-fair-queuing virtual finish tag.
 */
final class QueuedTask {

    final String id;
    final Callable<?> callable;
    final double tag;

    QueuedTask(String id, Callable<?> callable, double tag) {
        this.id = id;
        this.callable = callable;
        this.tag = tag;
    }
}
