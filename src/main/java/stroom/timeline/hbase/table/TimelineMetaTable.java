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
import org.apache.hadoop.hbase.util.Bytes;
import stroom.timeline.hbase.HBaseConnection;

public class TimelineMetaTable extends AbstractTable {

    private final String LONG_NAME = "MataData";
    private final String SHORT_NAME = "tm";
    private static final String COL_FAMILY_DATA = "d";
    private final HBaseConnection hBaseConnection;
    private final byte[] bShortName = Bytes.toBytes(SHORT_NAME);
    private final TableName tableName = TableName.valueOf(bShortName);

    public TimelineMetaTable(HBaseConnection hBaseConnection) {
        super(hBaseConnection);
        this.hBaseConnection = hBaseConnection;
    }

    //TODO have the rowkey as <id><name> which makes it easier to lookup by name

    @Override
    HTableDescriptor getTableDesctptor() {
                HColumnDescriptor dataFamily = new HColumnDescriptor(COL_FAMILY_DATA);

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName)
                .addFamily(dataFamily);
        return tableDescriptor;
    }


    @Override
    String getLongName() {
        return LONG_NAME;
    }

    @Override
    String getShortName() {
        return SHORT_NAME;
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
