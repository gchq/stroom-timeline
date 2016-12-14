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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.timeline.test.AbstractTest;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestBufferedStream extends AbstractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestBufferedStream.class);

    @Test
    public void getStream() throws Exception {

        BlockingQueue<Long> queue = new LinkedBlockingQueue<>();

        Duration timeout = Duration.ofSeconds(50);
        Duration topUpRetryTime = Duration.ofMillis(0);

        final AtomicLong supplierCounter = new AtomicLong();
        final AtomicBoolean doesReturnItems = new AtomicBoolean(true);

        //supply a batch of longs
        Supplier<Stream<Long>> itemSupplier = () -> {
            Stream<Long> stream;
            //alternately return items or nothing
            if (doesReturnItems.get()) {
                stream = IntStream.rangeClosed(1, 5)
                        .boxed()
                        .map(val -> supplierCounter.incrementAndGet())
                        .peek(val -> {
                            LOGGER.info("Supplying value {}", val);
                            doSleep(1000, "");
                        });
            } else {
                LOGGER.info("Supplying empty stream");
                stream = Stream.empty();
//                doSleep(100);
            }
            doesReturnItems.set(!doesReturnItems.get());
            return stream;
        };


        Stream<Long> stream = new BufferedStream(queue, timeout, itemSupplier, topUpRetryTime).stream();

        List<Long> list;
        try {
            list = stream
                    .peek(item -> {
                        LOGGER.info("Pulled item {} from queue", item.toString());
                        if (supplierCounter.get() > 30) {
                            throw new RuntimeException("Stopping the stream");
                        }
                    })
                    .collect(Collectors.toList());

            Assert.assertEquals(30, list.size());
        } catch (Exception e) {
            //do nothing
        }
    }

    private void doSleep(int ms, String msg) {
        try {
            if (msg != null & !msg.isEmpty()) {
                LOGGER.info(msg);
            }
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interupted");
        }
    }

}