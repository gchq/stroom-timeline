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
 */
package stroom.timeline.hbase.table;


import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import stroom.timeline.model.*;
import stroom.timeline.model.identifier.LongSequentialIdentifier;
import stroom.timeline.properties.MockPropertyService;
import stroom.timeline.api.TimelineView;
import stroom.timeline.hbase.HBaseTimelineView;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TestTimelineTable extends AbstractTableTest {


    MockPropertyService propertyService = new MockPropertyService();

    @Test
    public void testPutEvents() throws InterruptedException, IOException {

        List<OrderedEvent> orderedEvents = new ArrayList<>();

        OrderedEvent event1 = new OrderedEvent(Instant.now().minus(5, ChronoUnit.MINUTES), new byte[]{1, 2, 3});
        OrderedEvent event2 = new OrderedEvent(Instant.now().minus(10, ChronoUnit.MINUTES), new byte[]{1, 2, 3});

        orderedEvents.add(event1);
        orderedEvents.add(event2);

        TimelineTable timelineTable = getTimelineTable();

        timelineTable.putEvents(orderedEvents);

        Assert.assertEquals(2, super.hBaseTestUtilConnection.gethBaseTestingUtility().countRows(timelineTable.getTable()));
    }

    private TimelineTable getTimelineTable() {
        Timeline timeline = new Timeline("Timeline1", Duration.ofDays(400)).assignId(1);

        return new TimelineTable(timeline, super.hBaseTestUtilConnection);
    }


    @Test
    public void testFecthEvents() throws InterruptedException {

        //put a load of events into the table, in a random order

        ZonedDateTime now = ZonedDateTime.now();
        Instant startTime = Instant.now();
        List<OrderedEvent> events = new ArrayList<>();

        final AtomicLong counter = new AtomicLong(0);
        final int eventCount = 1000;

        List<OrderedEvent> randomEvents = new Random()
                .longs(eventCount, 0, 365)
                .boxed()
                .map(day -> {
                    Instant eventTime = now.minusDays(day).toInstant();
                    byte[] content = Bytes.toBytes(counter.incrementAndGet());
                    LongSequentialIdentifier idProvider = new LongSequentialIdentifier(counter.get());
                    return new OrderedEvent(eventTime, content, idProvider);
                })
                .collect(Collectors.toList());

//        randomEvents.stream().forEach(System.out::println);

//        System.out.println("---------------------------");

        List<Event> eventsInOrder = randomEvents.stream()
                .sorted()
                .map(OrderedEvent::getEvent)
                .collect(Collectors.toList());

//        eventsInOrder.stream().forEach(System.out::println);

        Timeline timeline = new Timeline("Timeline1", Duration.ofDays(400), 4, Duration.ofMillis(250))
                .assignId(1);

        TimelineTable timelineTable = new TimelineTable(timeline, super.hBaseTestUtilConnection);

        TimelineView timelineView = HBaseTimelineView.builder(timeline).build();

        timelineTable.putEvents(randomEvents);

        //fetch all events from the table and compare to a sorted list of the input events
        List<Event> eventsFromFetch = timelineTable.fetchEvents(timelineView, 5000);

        Assert.assertEquals(eventCount, eventsInOrder.size());
        Assert.assertEquals(eventCount, eventsFromFetch.size());

        Assert.assertEquals(eventsInOrder, eventsFromFetch);

    }


}