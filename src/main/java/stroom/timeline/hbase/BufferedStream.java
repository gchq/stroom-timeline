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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;


/**
 * The head of the queue is the oldest item
 */
public class BufferedStream<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedStream.class);

    private final BlockingQueue<T> queue;
    private final Duration timeout;
    private final Supplier<Stream<T>> itemsSupplier;
    private final Duration topUpRetryInterval;

    public BufferedStream(final BlockingQueue<T> queue, final Duration timeout, final Supplier<Stream<T>> itemsSupplier, final Duration topUpRetryInterval) {
        this.queue = queue;
        this.timeout = timeout;
        this.itemsSupplier = itemsSupplier;
        this.topUpRetryInterval = topUpRetryInterval;
    }

    public Stream<T> stream() {
        Supplier<T> supplier = () -> {
            //The stream consumer is trying to get another item off the stream
            if (queue.isEmpty()) {
                //initiate an asynchronous top up of the queue.  The Poll method below will handle waiting
                //for the items to appear in the queue
                ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
                executorService.submit(this::topUpQueue);
            }
            //We are the only thread on this queue so now it has been topped up we can poll.
            //The top up may have added nothing if the timeout was reached in which case the stream will end
            try {
                return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //thread has been interrupted so give up and return nothing
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        };
       return Stream.generate(supplier);
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
                        LOGGER.error("topUpQueue interrupted");
                        break;
                    }
                } else {
                    //timeout reached so give up and return nothing
                    break;
                }
            }
        }
    }



}
