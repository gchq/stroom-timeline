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
     * For a given offset return a start row key for each salt value. Each start row key will
     * have an identical time portion but differ in terms of the salt part
     * @param timelineView
     * @return
     */
    public static List<RowKey> getAllStartKeys(final TimelineView timelineView){
        final List<RowKey> rowKeys = new ArrayList<>();
        //This takes the simplistic approach of applying the same time to each start key
        //when in reality each salt band would start from a different time. However
        //this does not matter as there will be no rows in front to incorrectly pick up
        for (final short salt : timelineView.getSalt().getAllSaltValues()) {
            final RowKey rowKey = getRowKey(salt, timelineView.getOffset());
            rowKeys.add(rowKey);
        }
        return rowKeys;
    }

    public static RowKey getRowKey(final byte[] bSalt, final Instant time) {
        final byte[] bEventTime = Bytes.toBytes(time.toEpochMilli());
        return new RowKey(bSalt, bEventTime);
    }

    public static RowKey getRowKey(final short salt, final Instant time) {
        final byte[] bSalt = Bytes.toBytes(salt);
        final byte[] bEventTime = Bytes.toBytes(time.toEpochMilli());
        return new RowKey(bSalt, bEventTime);
    }
    /**
     * @return A row key for the timeline and a point on that timeline
     */
    public static RowKey getRowKey(final Timeline timeline, final Instant time) {
        final short salt = timeline.getSalt().computeSalt(time);
        return getRowKey(salt, time);
    }

    public static RowKey getRowKey(final Timeline timeline, final Event event) {
        return getRowKey(timeline, event.getEventTime());
    }


    public static short getSalt(final RowKey rowKey) {
        return Bytes.toShort(rowKey.getSaltBytes());

    }

    public static Instant getEventTime(final RowKey rowKey) {
        return Instant.ofEpochMilli(Bytes.toLong(rowKey.getEventTimeBytes()));

    }

}
