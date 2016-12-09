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
package stroom.timeline.hbase.adapters;

import org.apache.hadoop.hbase.util.Bytes;
import stroom.timeline.api.TimelineView;
import stroom.timeline.hbase.structure.RowKey;
import stroom.timeline.model.Event;
import stroom.timeline.model.Timeline;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class RowKeyAdapter {


    /**
     * @return The row key for the current offset of the timeline view
     */
    public static RowKey getRowKey(TimelineView timelineView) {
        return getRowKey(timelineView.getTimeline(), timelineView.getOffset());
    }

    public static List<RowKey> getAllRowKeys(TimelineView timelineView){
        List<RowKey> rowKeys = new ArrayList<>();
        for (short salt : timelineView.getSalt().getAllSaltValues()) {
            RowKey rowKey = getRowKey(salt, timelineView.getOffset());
            rowKeys.add(rowKey);
        }
        return rowKeys;
    }

    public static RowKey getRowKey(byte[] bSalt, Instant time) {
        byte[] bEventTime = Bytes.toBytes(time.toEpochMilli());
        return new RowKey(bSalt, bEventTime);
    }

    public static RowKey getRowKey(short salt, Instant time) {
        byte[] bSalt = Bytes.toBytes(salt);
        byte[] bEventTime = Bytes.toBytes(time.toEpochMilli());
        return new RowKey(bSalt, bEventTime);
    }
    /**
     * @return A row key for the timeline and a point on that timeline
     */
    public static RowKey getRowKey(Timeline timeline, Instant time) {
        short salt = timeline.getSalt().computeSalt(time);
        return getRowKey(salt, time);
    }

    public static RowKey getRowKey(Timeline timeline, Event event) {
        return getRowKey(timeline, event.getEventTime());
    }


    public static short getSalt(RowKey rowKey) {
        return Bytes.toShort(rowKey.getSaltBytes());

    }

    public static Instant getEventTime(RowKey rowKey) {
        return Instant.ofEpochMilli(Bytes.toLong(rowKey.getEventTimeBytes()));

    }

}
