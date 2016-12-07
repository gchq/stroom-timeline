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


import org.junit.Assert;
import org.junit.Test;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.model.Timeline;
import stroom.timeline.properties.MockPropertyService;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class TestTimelineTable extends  AbstractTableTest {


    MockPropertyService propertyService = new MockPropertyService();

    @Test
    public void testPutEvents() throws InterruptedException, IOException {

        List<OrderedEvent> orderedEvents = new ArrayList<>();

        OrderedEvent event1 = new OrderedEvent(Instant.now().minus(5, ChronoUnit.MINUTES), new byte[] {1,2,3});
        OrderedEvent event2 = new OrderedEvent(Instant.now().minus(10, ChronoUnit.MINUTES), new byte[] {1,2,3});

        orderedEvents.add(event1);
        orderedEvents.add(event2);

        Timeline timeline = new Timeline(1, "Timeline1", Duration.ofSeconds(1), Duration.ofHours(1), 1);

        TimelineTable timelineTable = new TimelineTable(timeline, 0, super.hBaseTestUtilConnection,  propertyService);

        timelineTable.putEvents(orderedEvents);

        Assert.assertEquals(2, super.hBaseTestUtilConnection.gethBaseTestingUtility().countRows(timelineTable.getTable()));
    }

}