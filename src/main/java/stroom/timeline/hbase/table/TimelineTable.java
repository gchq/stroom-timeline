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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.timeline.api.TimelineView;
import stroom.timeline.hbase.HBaseConnection;
import stroom.timeline.hbase.adapters.QualifiedCellAdapter;
import stroom.timeline.hbase.adapters.RowKeyAdapter;
import stroom.timeline.hbase.structure.QualifiedCell;
import stroom.timeline.hbase.structure.RowKey;
import stroom.timeline.model.Event;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.model.Timeline;
import stroom.timeline.util.ByteArrayUtils;
import stroom.timeline.util.LogUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * DAO for the Timeline HBase table
 * <p>
 * This is how the data is stored, assuming a contiguous data set.
 * <p>
 * |   min 1   |  min 2    | min 3     |
 * salt|---------------time-------------------->
 * 0   | ###         ###         ###
 * 1   |    ###         ###         ###
 * 2   |       ###         ###         ###
 * 3   |          ###         ###         ###
 * <p>
 * If we use the modulus for the salt then this would spread the values
 * all over the place and mean a lot of sorting overhead on retrieval.
 * Instead time ranges have a static salt value so that we can grab a range
 * from each salt value and then just sort the small number of batches by their
 * salt value.
 * <p>
 * The aim is for each set of salt values to reside in its own region to spread
 * the load across region servers. This does mean a scan over a time range means
 * issuing multiple scans (one for each salt) and then assembling the data on receipt.
 */
public class TimelineTable extends AbstractTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimelineTable.class);

    static final String LONG_NAME_PREFIX = "Timeline";
    static final String SHORT_NAME_PREFIX = "t";
    static final byte[] COL_FAMILY_CONTENT = Bytes.toBytes("c");
    static final byte[] COL_FAMILY_META = Bytes.toBytes("m");
    static final byte[] COL_ROW_CHANGE_NUMBER = Bytes.toBytes("RCN");
    private final Timeline timeline;
    private final HBaseConnection hBaseConnection;
    private final String shortName;
    private final byte[] bShortName;
    private final TableName tableName;

    //TODO with a TTL on the col family that will work off the insertion time rather than the event time
    //hopefully this will be reasonable enough as most data will come in within a few minutes of the event time


    public TimelineTable(Timeline timeline, HBaseConnection hBaseConnection) {
        super(hBaseConnection);

        Preconditions.checkNotNull(timeline);

        this.timeline = timeline;
        this.hBaseConnection = hBaseConnection;
        shortName = SHORT_NAME_PREFIX + timeline.getId();
        bShortName = Bytes.toBytes(shortName);
        tableName = TableName.valueOf(bShortName);
        initialiseTable();
    }


    public void putEvents(Collection<OrderedEvent> orderedEvents) {
        Preconditions.checkNotNull(orderedEvents);

        //Work out the retention cut off date so we can ignore events before it
        Instant notBeforeTime = Instant.now()
                .minus(timeline.getRetention().orElse(Duration.ZERO));

        //keep a table object per thread, keyed on thread id as the table objects
        //are not thread safe. It is debatable whether we should just call getTable before
        //each put.
        ConcurrentMap<Long, Table> tableMap = new ConcurrentHashMap<>();
        try {
            //Multiple threads will convert the events into puts and call put on the table.
            //HBase should buffer the puts in its client code.
            orderedEvents.stream()
                    .parallel()
                    .filter(event -> event.getEventTime().isAfter(notBeforeTime))
                    .map(event -> QualifiedCellAdapter.getQualifiedCell(timeline, event))
                    .map(qualifiedCellToPutMapper)
                    .forEach(put -> {
                        try {
                            Table table = tableMap.computeIfAbsent(Thread.currentThread().getId(), key -> getTable());
                            table.put(put);
                        } catch (IOException e) {
                            throw new RuntimeException("Error writing data to HBase", e);
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error while trying persist a collection of orderedEvents of size %s", orderedEvents.size()), e);
        } finally {
            tableMap.forEach((id, table) -> closeTable(table));
        }
    }


    public List<Event> fetchEvents(TimelineView timelineView, int rowCount) {
        Preconditions.checkNotNull(timelineView);
        Preconditions.checkArgument(rowCount >= 1, "rowCount must be >= 1");

        LOGGER.info("fetchEvents called for timeline {} and rowCount {}", timelineView, rowCount);

        if (!timeline.equals(timelineView.getTimeline())) {
            throw new RuntimeException(String.format("TimelineView %s is for a different timeline to this %s",
                    timelineView.getTimeline().getId(), timeline.getId()));
        }

        //a row may be one event or many events depending on the data, i.e. if many events have
        //the same event time (to the granularity of the rowkey) then you could get hundreds/thousands
        //of columns on the row. If we are using salt prefixes then need to divide the desired row
        //count by the number of salts to get a row limit per scan (salt).
        final int scanMaxResults = rowCount / timelineView.getTimeline().getSalt().getSaltCount();

        //get a function to add a stop key if required
        BiConsumer<Scan, RowKey> stopKeyFunction = getStopRowFunction(timelineView);

        //Initiate a table scan for each salt in parallel. Each scan will fetch blocks of
        //ordered events, with a gap between each block. Once all the blocks have been captured
        //from each scan the blocks need to be assembled into time order to produce a
        //single list of ordered events.
        List<Event> events = RowKeyAdapter.getAllStartKeys(timelineView)
                .parallelStream()
                .map(startKey -> startKeyToEventsBatchMapper(scanMaxResults, stopKeyFunction, startKey))
                .flatMap(Set::stream)
                .sorted(Map.Entry.comparingByKey())
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toList());

        return events;
    }

    /**
     * For a given start key, scanMaxResults
     */
    private Set<Map.Entry<Long, List<Event>>> startKeyToEventsBatchMapper(
            final int scanMaxResults, final BiConsumer<Scan, RowKey> stopRowFunction, final RowKey startKey) {

        //Use a prefix filter to ensure we do start scanning into another salt's values
        PrefixFilter prefixFilter = new PrefixFilter(startKey.getSaltBytes());

        //The salted key will ensure the scan only goes to a single region
        Scan scan = new Scan(startKey.getRowKeyBytes())
                .addFamily(COL_FAMILY_CONTENT)
                .setMaxResultSize(scanMaxResults)
                .setFilter(prefixFilter);

        LogUtils.traceIfEnabled(LOGGER, () -> {
            LOGGER.trace("Creating scan for start key {} and max rows of {}",
                    ByteArrayUtils.byteArrayToAllForms(startKey.getRowKeyBytes()), scanMaxResults);
        });

        stopRowFunction.accept(scan, startKey);

        final Map<Long, List<Event>> eventsForSalt;

        try (final Table table = hBaseConnection.getConnection().getTable(tableName)) {
            ResultScanner resultScanner = table.getScanner(scan);
            eventsForSalt = StreamSupport.stream(resultScanner.spliterator(), false)
                    .sequential()
                    .flatMap(this::scanResultToSaltedEventStreamConverter)
                    .collect(Collectors.groupingBy(
                            SaltedEvent::getTimeBucketNo,
                            Collectors.mapping(
                                    SaltedEvent::getEvent,
                                    Collectors.toList())));
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error while trying to fetch %s rows from timeline %s with salt %s",
                    scanMaxResults, timeline, RowKeyAdapter.getSalt(startKey)), e);
        }

        LogUtils.traceIfEnabled(LOGGER, () -> {
            eventsForSalt.keySet().stream()
                    .sorted()
                    .map(val -> "timeBucketNo: " + val.toString())
                    .forEach(LOGGER::trace);
        });
        return eventsForSalt.entrySet();
    }

    private Stream<SaltedEvent> scanResultToSaltedEventStreamConverter(final Result scanResult) {

        //scanResult represents a single row, so decode the rowkey
        //then get all the cells for the row, adding them
        //to a collection
        byte[] bRowKey = scanResult.getRow();
        RowKey rowKey = new RowKey(bRowKey);
        Instant rowTime = RowKeyAdapter.getEventTime(rowKey);
        long timeBucketNo = timeline.getSalt().getTimeBucketNo(rowTime);

        LogUtils.traceIfEnabled(LOGGER, () -> LOGGER.trace("Cell count on row {} is {}",
                rowKey.toString(), scanResult.listCells().size()));

        //return a stream of event objects
        Stream<SaltedEvent> eventStream = scanResult.listCells()
                .stream()
                .map(cell -> {
                    byte[] content = CellUtil.cloneValue(cell);
                    Event event = new Event(rowTime, content);
                    return new SaltedEvent(timeBucketNo, event);
                });
        return eventStream;
    }

    private BiConsumer<Scan, RowKey> getStopRowFunction(final TimelineView timelineView) {

        //The time that all delay calculations are based off for consistency
        final Instant now = Instant.now();

        //TODO need to set a stop row so we get one contiguous chunk of rows for a
        //time bucket and not jump onto another time bucket.
        //for now as the salt is hard coded it doesn't matter
        //This current code is too simplistic as it only deals with the delay and not the
        //salting
        if (!timelineView.getDelay().equals(Duration.ZERO)) {
            final Instant notBeforeTime = now.minus(timelineView.getDelay());
            LOGGER.trace("Adding stop row for notBeforeTime: {}", notBeforeTime);
            return (scan, startKey) -> {
                final RowKey stopKey = RowKeyAdapter.getRowKey(startKey.getSaltBytes(), notBeforeTime);
                scan.setStopRow(stopKey.getRowKeyBytes());
            };
        } else {
            //no stop key to be applied
            return (scan, startKey) -> {};
        }
    }

    @Override
    HTableDescriptor getTableDesctptor() {
        HColumnDescriptor metaFamily = new HColumnDescriptor(COL_FAMILY_META);

        //compress the content family as each cell value can be quite large
        //and should compress well
        //TODO currently set to GZ compression as SNAPPY does not seem to be supported
        //by the HBaseTEstingUtility. Need to see if we want to change to SNAPPY (quicker but
        //doesn't compress as well as GZ) and if so how we can get it working in junits or test
        //for its presence.
        HColumnDescriptor contentFamily = new HColumnDescriptor(COL_FAMILY_CONTENT)
                .setCompressionType(Compression.Algorithm.GZ);

        timeline.getRetention().ifPresent(retention -> {
            metaFamily.setTimeToLive(Math.toIntExact(retention.getSeconds()));
            contentFamily.setTimeToLive(Math.toIntExact(retention.getSeconds()));
        });

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName)
                .addFamily(metaFamily)
                .addFamily(contentFamily);

        return tableDescriptor;
    }

    @Override
    Optional<byte[][]> getRegionSplitKeys() {
        short[] salts = timeline.getSalt().getAllSaltValues();
        if (salts.length > 1) {
            //we want the salt values that HBase will be the split points so ignore the
            //first salt value, i.e. 4 salts = 3 split points
            short[] splitKeySalts = Arrays.copyOfRange(salts, 1, salts.length);
            byte[][] splitKeys = new byte[splitKeySalts.length][];
            for (int i = 0; i < splitKeySalts.length; i++) {
                splitKeys[i] = Bytes.toBytes(splitKeySalts[i]);
            }
            return Optional.of(splitKeys);
        } else {
            //only one salt so no point splitting regions
            return Optional.empty();
        }
    }

    @Override
    String getLongName() {
        return LONG_NAME_PREFIX + "-" + timeline.getName();
    }

    @Override
    String getShortName() {
        return shortName;
    }

    @Override
    byte[] getShortNameBytes() {
        return bShortName;
    }

    @Override
    TableName getTableName() {
        return tableName;
    }

    /**
     * Convert a QualifiedCell object into a stream of HBase table Mutation objects
     */
    private final Function<QualifiedCell, Put> qualifiedCellToPutMapper = qualifiedCell -> {

        RowKey rowKey = qualifiedCell.getRowKey();
        long eventTimeMs = RowKeyAdapter.getEventTime(rowKey).toEpochMilli();
        byte[] bRowKey = rowKey.getRowKeyBytes();

        //do the put with the event time as the cell timestamp, instead of HBase just using the default now()
        //this is so the aging off of cells is done based on event time, not insert time
        Put eventPut = new Put(bRowKey)
                .addColumn(COL_FAMILY_CONTENT, qualifiedCell.getColumnQualifier(), eventTimeMs, qualifiedCell.getValue());

        LogUtils.traceIfEnabled(LOGGER, () -> {
            LOGGER.trace("Creating put, rowKey: {}, colQual: {}",
                    rowKey.toString(), ByteArrayUtils.byteArrayToAllForms(qualifiedCell.getColumnQualifier()));
        });

        return eventPut;
    };

    public static class SaltedEvent implements Comparable<SaltedEvent> {

        private final long timeBucketNo;
        private final Event event;

        public SaltedEvent(long timeBucketNo, Event event) {
            this.timeBucketNo = timeBucketNo;
            this.event = event;
        }

        public Long getTimeBucketNo() {
            return timeBucketNo;
        }

        public Event getEvent() {
            return event;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SaltedEvent that = (SaltedEvent) o;

            return timeBucketNo == that.timeBucketNo;
        }

        @Override
        public int hashCode() {
            return (int) (timeBucketNo ^ (timeBucketNo >>> 32));
        }

        @Override
        public int compareTo(SaltedEvent o) {
            return 0;
        }
    }

    private static class EventsBucket implements Comparable<EventsBucket> {
        private final List<Event> events;
        private final byte[] bSalt;
        private final Instant firstEventTime;

        /**
         * @param events A list of events supplied sorted by the event time
         * @param bSalt  The salt value in bytes for this batch of events
         */
        public EventsBucket(final List<Event> events, final byte[] bSalt) {
            this.events = events;
            this.bSalt = bSalt;
            this.firstEventTime = events.isEmpty() ? Instant.EPOCH : events.get(0).getEventTime();
        }

        public Stream<Event> stream() {
            return events.stream();
        }

        public byte[] getSalt() {
            return bSalt;
        }

        @Override
        public int compareTo(EventsBucket other) {
//            return Bytes.compareTo(this.bSalt, other.bSalt);
            //All events in the
            return firstEventTime.compareTo(other.firstEventTime);
        }
    }
}
