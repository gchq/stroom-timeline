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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.timeline.hbase.HBaseConnection;
import stroom.timeline.hbase.adapters.QualifiedCellAdapter;
import stroom.timeline.hbase.structure.QualifiedCell;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.model.Timeline;
import stroom.timeline.properties.PropertyService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

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

    public void putEvents(Collection<OrderedEvent> orderedEvents) throws InterruptedException {

        try (final Table table = hBaseConnection.getConnection().getTable(tableName)){
            orderedEvents.stream()
                    .map(QualifiedCellAdapter::getQualifiedCell)
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
}
