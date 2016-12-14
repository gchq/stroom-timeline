/*
 * Copyright 2016 Crown Copyright
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
 *
 */

package stroom.timeline.hbase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/*
 * This class is based on code from http://stackoverflow.com/questions/23462209/stream-api-and-queues-in-java-8
 */

/**
 * The head of the queue is the oldest item
 */
public class QueueSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<T> queue;
    private final Duration timeout;
    private final Supplier<List<T>> itemsSupplier;
    private final Duration topUpRetryInterval;

    public QueueSpliterator(final BlockingQueue<T> queue, final Duration timeout, final Supplier<List<T>> itemsSupplier, final Duration topUpRetryInterval) {
        this.queue = queue;
        this.timeout = timeout;
        this.itemsSupplier = itemsSupplier;
        this.topUpRetryInterval = topUpRetryInterval;
    }

    @Override
    public int characteristics() {
        return Spliterator.NONNULL | Spliterator.ORDERED | Spliterator.SORTED;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        if (queue.isEmpty()) {
            topUpQueue();
        }
        //We are the only thread on this queue so now it has been topped up we can poll.
        //The top up may have added nothing if the timeout was reached in which case the stream will end
        final T next = queue.poll();
        if (next == null) {
            return false;
        }
        action.accept(next);
        return true;
    }

    private void topUpQueue() {
        List<T> items = itemsSupplier.get();

        Instant startTime = Instant.now();

        //Keep trying to get new items
        while (items.isEmpty()) {
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            final FutureTask<List<T>> futureTask = new FutureTask<List<T>>(itemsSupplier::get);
            scheduledExecutorService.schedule(futureTask, 1, TimeUnit.SECONDS);
            Duration timeSinceStart = Duration.between(startTime, Instant.now());
            if (!timeSinceStart.isNegative() && timeSinceStart.compareTo(timeout) < 0) {
                long taskTimeoutMs = timeout.minus(timeSinceStart).toMillis();
                try {
                    scheduledExecutorService.awaitTermination(taskTimeoutMs, TimeUnit.MILLISECONDS);
                    try {
                        items = futureTask.get();
                    } catch (ExecutionException e) {
                        throw new RuntimeException("Error attempting to top up the queue", e);
                    }
                } catch (InterruptedException e) {
                    //thread has been interupted so give up and return nothing
                    Thread.currentThread().interrupt();
                    break;
                }

            } else {
                //timeout reached so give up and return nothing
                break;
            }
        }

        //items.get(0) is the oldest item in the list
        //top up the queue with all the items
        items.forEach(queue::add);
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

}
