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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.timeline.api.TimelineView;
import stroom.timeline.hbase.HBaseTimelineViewBuilder;
import stroom.timeline.model.Event;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.model.Timeline;
import stroom.timeline.model.identifier.LongSequentialIdentifier;
import stroom.timeline.properties.MockPropertyService;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestTimelineTable extends AbstractTableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTimelineTable.class);

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
    public void testFecthEvents_singleSalt() throws Exception {

        //put a load of events into the table, in a random order and make sure they come back
        //out in order

        ZonedDateTime now = ZonedDateTime.now();
        Instant startTime = Instant.now();

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

        Timeline timeline = new Timeline("Timeline1", Duration.ofDays(400))
                .assignId(1);

        doPutThenFetch(randomEvents, timeline);
    }

    @Test
    public void testFecthEvents_fourSalts_oneBandPerSalt_oneEventsPerSalt() throws Exception {

        final ZonedDateTime startTime = ZonedDateTime.of(2016,12,13,10,35,0,0, ZoneOffset.UTC);
        final int saltCount = 4;
        final Duration saltRange = Duration.ofSeconds(1);
        final int eventsPerSaltRange = 1;
        final int totalEvents = eventsPerSaltRange * saltCount;
        final Duration eventDelta = saltRange.dividedBy(eventsPerSaltRange);

        final AtomicLong counter = new AtomicLong(0);
        List<OrderedEvent> randomEvents = LongStream.range(0, totalEvents)
                .boxed()
                .map(i -> {
                    Instant eventTime = startTime.plus(eventDelta.multipliedBy(i)).toInstant();
                    byte[] content = Bytes.toBytes(counter.incrementAndGet());
                    LongSequentialIdentifier idProvider = new LongSequentialIdentifier(counter.get());
                    return new OrderedEvent(eventTime, content, idProvider);
                })
                .collect(Collectors.toList());

        Timeline timeline = new Timeline("Timeline1", Duration.ofDays(1000), saltCount, saltRange)
                .assignId(1);

        doPutThenFetch(randomEvents, timeline);
    }

    @Test
    public void testFecthEvents_fourSalts_oneBandPerSalt_fourEventsPerSalt() throws Exception {

        final ZonedDateTime startTime = ZonedDateTime.of(2016,12,13,10,35,0,0, ZoneOffset.UTC);
        final int saltCount = 4;
        final Duration saltRange = Duration.ofSeconds(1);
        final int eventsPerSaltRange = 4;
        final int totalEvents = eventsPerSaltRange * saltCount;
        final Duration eventDelta = saltRange.dividedBy(eventsPerSaltRange);

        final AtomicLong counter = new AtomicLong(0);
        List<OrderedEvent> randomEvents = LongStream.range(0, totalEvents)
                .boxed()
                .map(i -> {
                    Instant eventTime = startTime.plus(eventDelta.multipliedBy(i)).toInstant();
                    byte[] content = Bytes.toBytes(counter.incrementAndGet());
                    LongSequentialIdentifier idProvider = new LongSequentialIdentifier(counter.get());
                    return new OrderedEvent(eventTime, content, idProvider);
                })
                .collect(Collectors.toList());

        Timeline timeline = new Timeline("Timeline1", Duration.ofDays(1000), saltCount, saltRange)
                .assignId(1);

        doPutThenFetch(randomEvents, timeline);
    }

    @Test
    public void testFecthEvents_fourSalts_twoBandsPerSalt_fourEventsPerSalt() throws Exception {

        final ZonedDateTime startTime = ZonedDateTime.of(2016,12,13,10,35,0,0, ZoneOffset.UTC);
        final int saltCount = 4;
        final Duration saltRange = Duration.ofSeconds(1);
        final int eventsPerSaltRange = 4;

        //TODO this test is WIP, need to add multiple bands logic
        Assert.assertTrue(false);
        final int totalEvents = eventsPerSaltRange * saltCount;
        final Duration eventDelta = saltRange.dividedBy(eventsPerSaltRange);

        final AtomicLong counter = new AtomicLong(0);
        List<OrderedEvent> randomEvents = LongStream.range(0, totalEvents)
                .boxed()
                .map(i -> {
                    Instant eventTime = startTime.plus(eventDelta.multipliedBy(i)).toInstant();
                    byte[] content = Bytes.toBytes(counter.incrementAndGet());
                    LongSequentialIdentifier idProvider = new LongSequentialIdentifier(counter.get());
                    return new OrderedEvent(eventTime, content, idProvider);
                })
                .collect(Collectors.toList());

        Timeline timeline = new Timeline("Timeline1", Duration.ofDays(1000), saltCount, saltRange)
                .assignId(1);

        doPutThenFetch(randomEvents, timeline);
    }

    public List<Event> doPutThenFetch(List<OrderedEvent> events, Timeline timeline) throws InterruptedException, IOException {

        //put a load of events into the table, in a random order
        List<Event> eventsInOrder = events.stream()
                .sorted()
                .map(OrderedEvent::getEvent)
                .collect(Collectors.toList());

        TimelineTable timelineTable = new TimelineTable(timeline, super.hBaseTestUtilConnection);

        TimelineView timelineView = new HBaseTimelineViewBuilder(timeline, timelineTable).build();

        timelineTable.putEvents(events);

        long cellCount = hBaseTestUtilConnection.getCellCount(TimelineTable.SHORT_NAME_PREFIX + timeline.getId(), TimelineTable.COL_FAMILY_CONTENT );

        LOGGER.info("Cell count: {}", cellCount);

        //fetch all events from the table and compare to a sorted list of the input events
        List<Event> eventsFromFetch = timelineTable.fetchEvents(timelineView, 5000);

        if (events.size() <= 100) {
            logList(events, "Source events");
            logList(eventsInOrder, "Source events (ordered)");
            logList(eventsFromFetch, "Events from fetch");
        }

        Assert.assertEquals(events.size(), eventsInOrder.size());
        Assert.assertEquals(events.size(), eventsFromFetch.size());

        Assert.assertEquals(eventsInOrder, eventsFromFetch);

        return eventsFromFetch;
    }

    private <T> void logList(List<T> list, String listName) {
        LOGGER.debug(listName + ":");
        list.stream()
                .map(T::toString)
                .forEach(LOGGER::debug);

    }


}