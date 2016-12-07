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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.timeline.hbase.HBaseConnection;
import stroom.timeline.hbase.adapters.QualifiedCellAdapter;
import stroom.timeline.hbase.adapters.RowKeyAdapter;
import stroom.timeline.hbase.structure.QualifiedCell;
import stroom.timeline.hbase.structure.RowKey;
import stroom.timeline.model.Event;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.model.Timeline;
import stroom.timeline.model.TimelineView;
import stroom.timeline.properties.PropertyService;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TimelineTable extends AbstractTable {


    private static final String LONG_NAME_PREFIX = "Timeline";
    private static final String SHORT_NAME_PREFIX = "t";
    private static final byte[] COL_FAMILY_CONTENT = Bytes.toBytes("c");
    private static final byte[] COL_FAMILY_META = Bytes.toBytes("m");
    private static final byte[] COL_ROW_CHANGE_NUMBER = Bytes.toBytes("RCN");
    private final Timeline timeline;
    private final int timelineCode;
    private final HBaseConnection hBaseConnection;
    private final PropertyService propertyService;
    private final String shortName;
    private final byte[] bShortName;
    private final TableName tableName;

    //TODO with a TTL on the col family that will work off the insertion time rather than the event time
    //hopefully this will be reasonable enough as most data will come in within a few minutes of the event time


    public TimelineTable(Timeline timeline, int timelineCode, HBaseConnection hBaseConnection, PropertyService propertyService) {
        super(hBaseConnection, propertyService);
        this.timeline = timeline;
        this.timelineCode = timelineCode;
        this.hBaseConnection = hBaseConnection;
        this.propertyService = propertyService;
        shortName = SHORT_NAME_PREFIX + timelineCode;
        bShortName = Bytes.toBytes(shortName);
        tableName = TableName.valueOf(bShortName);
        initialiseTable();
    }


    public void putEvents(Collection<OrderedEvent> orderedEvents) throws InterruptedException {

        try (final Table table = hBaseConnection.getConnection().getTable(tableName)){
            orderedEvents.stream()
                    .map(event -> QualifiedCellAdapter.getQualifiedCell(timeline,event))
                    .flatMap(qualifiedCellToMutationsMapper)
                    .forEach(mutation -> {
                        try {
                            if (mutation instanceof Increment){
                               table.increment((Increment)mutation);
                            } else if (mutation instanceof Put) {
                                table.put((Put)mutation);
                            } else {
                                throw new RuntimeException(String.format("Mutations is of a class %s we weren't expecting",mutation.getClass().getName()));
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Error writing data to HBase",e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error while trying persist a collection of orderedEvents of size %s", orderedEvents.size()), e);
        }
    }


    public List<Event> fetchEvents(TimelineView timelineView, int rowCount){

        if (!timeline.equals(timelineView.getTimeline())){
            throw new RuntimeException(String.format("TimelineView %s is for a different timeline to this %s",
                    timelineView.getTimeline().getId(), timeline.getId()));
        }

        int scanMaxResults = rowCount / timelineView.getTimeline().getSaltCount();

        //one scan per salt value
        List<Event> events = RowKeyAdapter.getAllRowKeys(timelineView)
                .parallelStream()
                .map(startKey -> {
                    //TODO need to set a stop row so we get one contiguous chunk of rows for a
                    //time bucket and not jump onto another time bucket.
                    //for now as the salt is hard coded it doesn't matter
                    Scan scan = new Scan(startKey.getRowKeyBytes())
                            .addFamily(COL_FAMILY_CONTENT)
                            .setMaxResultSize(scanMaxResults);

                    List<Event> eventsForSalt;
                    try (final Table table = hBaseConnection.getConnection().getTable(tableName)){
                        ResultScanner resultScanner = table.getScanner(scan);
                        eventsForSalt = StreamSupport.stream(resultScanner.spliterator(),false)
                                .sequential()
                                .flatMap(result -> {
                                    byte[] bRowKey = result.getRow();
                                    RowKey rowKey = new RowKey(bRowKey);
                                    byte[] bSalt = rowKey.getSaltBytes();
                                    Instant rowTime = RowKeyAdapter.getEventTime(rowKey);
                                    //return a stream of event objects
                                    Stream<Event> eventStream = result.listCells()
                                            .stream()
                                            .map(cell -> {
                                               byte[] content = CellUtil.cloneValue(cell);
                                               Event event = new Event(rowTime, content);
                                               return event;
                                            });
                                    return eventStream;

                                })
                                .collect(Collectors.toList());
                    } catch (IOException e) {
                        //TODO what happens to the other threads if we come in here
                        throw new RuntimeException(String.format("Error while trying to fetch %s rows from timeline %s with salt %s",
                                rowCount, timeline, RowKeyAdapter.getSalt(startKey)), e);
                    }
                    return new EventsBucket(eventsForSalt, startKey.getSaltBytes());
                })
                .sorted()
                .flatMap(eventsBucket -> eventsBucket.stream())
                .collect(Collectors.toList());

        return events;
    }




    @Override
    HTableDescriptor getTableDesctptor() {
        HColumnDescriptor metaFamily = new HColumnDescriptor(COL_FAMILY_META)
                .setTimeToLive(Math.toIntExact(timeline.getRetention().getSeconds()));
        HColumnDescriptor contentFamily = new HColumnDescriptor(COL_FAMILY_CONTENT)
                .setTimeToLive(Math.toIntExact(timeline.getRetention().getSeconds()));

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName)
                .addFamily(metaFamily)
                .addFamily(contentFamily);
        return tableDescriptor;
    }

    @Override
    String getLongName() {
        return LONG_NAME_PREFIX + "-" + timeline.getName();
    }

    @Override
    String getShortName() {
        return null;
    }

    @Override
    byte[] getShortNameBytes() {
        return bShortName;
    }

    @Override
    TableName getTableName() {
        return tableName;
    }

    private Function<QualifiedCell, Stream<Mutation>> qualifiedCellToMutationsMapper = qualifiedCell -> {

        List<Mutation> mutations = new ArrayList<>();

        byte[] rowKey = qualifiedCell.getRowKey().getRowKeyBytes();
        Put eventPut = new Put(rowKey)
                .addColumn(COL_FAMILY_CONTENT, qualifiedCell.getColumnQualifier(), qualifiedCell.getValue());

        //TODO do we care about having a row change number, probably adds a fair bit of cost and
        //we can't combine the put and increment into a single row op so will probably require two row locks
        Increment ecnIncrement = new Increment(rowKey)
                .addColumn(COL_FAMILY_META, COL_ROW_CHANGE_NUMBER, 1L);
        mutations.add(eventPut);
        mutations.add(ecnIncrement);

        return mutations.stream();
    };

    private static class EventsBucket implements  Comparable<EventsBucket> {
        private final List<Event> events;
        private final byte[] bSalt;

        public EventsBucket(List<Event> events, byte[] bSalt) {
            this.events = events;
            this.bSalt = bSalt;
        }

        public Stream<Event> stream() {
            return events.stream();
        }

        public byte[] getSalt() {
            return bSalt;
        }


        @Override
        public int compareTo(EventsBucket other) {
            return Bytes.compareTo(this.bSalt, other.bSalt);
        }
    }
}
