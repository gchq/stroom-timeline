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

package stroom.timeline.hbase.adapters;

import org.junit.Assert;
import org.junit.Test;
import stroom.timeline.api.TimelineView;
import stroom.timeline.hbase.HBaseTimelineView;
import stroom.timeline.hbase.structure.RowKey;
import stroom.timeline.model.Timeline;
import stroom.timeline.util.ByteArrayWrapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class TestRowKeyAdapter {

    @Test
    public void getAllStartKeys() throws Exception {

        int saltCount = 4;
        Timeline timeline = new Timeline("MyTimeline", Duration.ofDays(365), 4, Duration.ofMillis(250));
        TimelineView timelineView = HBaseTimelineView.builder(timeline).setDelay(Duration.ZERO).setOffset(Instant.now()).build();

        List<RowKey> startKeys = RowKeyAdapter.getAllStartKeys(timelineView);

        Assert.assertEquals(timeline.getSalt().getSaltCount(), startKeys.size());

        //Should all have the same time part
        Assert.assertEquals(1, startKeys.stream()
                .map(rowKey -> ByteArrayWrapper.fromBytes(rowKey.getEventTimeBytes()))
                .distinct()
                .count());

        //Should be 4 different salt values
        Assert.assertEquals(saltCount, startKeys.stream()
                .map(rowKey -> ByteArrayWrapper.fromBytes(rowKey.getSaltBytes()))
                .distinct()
                .count());

    }


}