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
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/*
 * This class is based on code from http://stackoverflow.com/questions/23462209/stream-api-and-queues-in-java-8
 */

/**
 * The head of the queue is the oldest item
 */
public class QueueSpliterator<T> implements Spliterator<T> {

    private final BlockingQueue<T> queue;
    private final Duration timeout;
    private final Supplier<Stream<T>> itemsSupplier;
    private final Duration topUpRetryInterval;

    public QueueSpliterator(final BlockingQueue<T> queue, final Duration timeout, final Supplier<Stream<T>> itemsSupplier, final Duration topUpRetryInterval) {
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
        //The stream consumer is trying to get another item off the stream
        if (queue.isEmpty()) {
            //initiate an asynchronous top up of the queue.  The Poll method below will handle waiting
            //for the items to appear in the queue
            ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.submit(this::topUpQueue);
        }
        //We are the only thread on this queue so now it has been topped up we can poll.
        //The top up may have added nothing if the timeout was reached in which case the stream will end
        final T next;
        try {
            next = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            //thread has been interrupted so give up and return nothing
            Thread.currentThread().interrupt();
            return false;
        }
        if (next == null) {
            return false;
        }
        action.accept(next);
        return true;
    }

    private void topUpQueue() {
        final AtomicLong counter = new AtomicLong();

        Runnable fetchAndAddTask = () -> {
            //attempt to top up the queue, keeping track of how many events we add
            //items.get(0) is the oldest item in the list
            itemsSupplier.get()
                    .peek(event -> counter.incrementAndGet())
                    .forEach(queue::add);
        };

        //First crack at fetching events
        fetchAndAddTask.run();

        if (counter.get() == 0) {
            Instant startTime = Instant.now();
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

            //Keep trying to get new items if we didn't succeed before
            while (counter.get() == 0L) {

                scheduledExecutorService.schedule(fetchAndAddTask, topUpRetryInterval.toMillis(), TimeUnit.MILLISECONDS);

                Duration timeSinceStart = Duration.between(startTime, Instant.now());
                if (!timeSinceStart.isNegative() && timeSinceStart.compareTo(timeout) < 0) {
                    long taskTimeoutMs = timeout.minus(timeSinceStart).toMillis();
                    try {
                        //ensure the task has completed
                        scheduledExecutorService.awaitTermination(taskTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        //thread has been interrupted so give up and return nothing
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    //timeout reached so give up and return nothing
                    break;
                }
            }
        }
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

}
