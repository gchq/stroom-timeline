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
import stroom.timeline.hbase.structure.QualifiedCell;
import stroom.timeline.hbase.structure.RowKey;
import stroom.timeline.model.Event;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.util.UuidAdapter;

import java.time.Instant;
import java.util.UUID;

public class QualifiedCellAdapter {

    private QualifiedCellAdapter() {
    }

    public static QualifiedCell getQualifiedCell(OrderedEvent orderedEvent){

        RowKey rowKey = RowKeyAdapter.getRowKey(orderedEvent.getEvent());

        byte[] colQualifier = orderedEvent.getSequentialIdentifier();


        QualifiedCell qualifiedCell = new QualifiedCell(rowKey, colQualifier, orderedEvent.getContent());

        return qualifiedCell;
    }

    public static Event getEvent(QualifiedCell qualifiedCell){

        UUID uuid = UuidAdapter.getUUIDFromBytes(qualifiedCell.getColumnQualifier());
        Instant eventTime = Instant.ofEpochMilli(Bytes.toLong(qualifiedCell.getRowKey().getEventTimeBytes()));
        Event event = new Event(eventTime, qualifiedCell.getValue());
        return  event;
    }
}
