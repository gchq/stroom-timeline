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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import stroom.timeline.hbase.HBaseConnection;

import java.io.IOException;
import java.util.Optional;

public abstract class AbstractTable {

    private final HBaseConnection hBaseConnection;

    public AbstractTable(HBaseConnection hBaseConnection) {
        Preconditions.checkNotNull(hBaseConnection);

        this.hBaseConnection = hBaseConnection;
    }

    void initialiseTable() {
        HTableDescriptor tableDescriptor = getTableDesctptor();

        Admin admin = hBaseConnection.getAdmin();
        boolean doesTableExist = false;
        try {
            doesTableExist = admin.tableExists(getTableName());
        } catch (IOException e) {
            throw new RuntimeException("Error testing existence of table " + getLongName(), e);
        }

        //Auto-create table on first use if it isn't there
        if (!doesTableExist) {
            try {
                Optional<byte[][]> splitKeys = getRegionSplitKeys();
                if (splitKeys.isPresent()) {
                    admin.createTable(tableDescriptor, splitKeys.get());
                } else {
                    admin.createTable(tableDescriptor);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error creating table " + getLongName(), e);
            }
        }
    }

    Table getTable() {
        try {
            return hBaseConnection.getConnection().getTable(getTableName());
        } catch (IOException e) {
            throw new RuntimeException("Error getting table object for table name " + getTableName().toString(), e);
        }
    }

    void closeTable(Table table) {
        try {
            table.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing table object for table name " + getTableName().toString(), e);
        }
    }

    /**
     * @return A HTableDescriptor that can be used to create the table
     */
    abstract HTableDescriptor getTableDesctptor();

    /**
     * @return If the table should be pre-split into regions then this method
     * should return an array of row key split keys. The size of the array should
     * be one less than the desired number of regions. i.e. the first split key in the
     * array is the start key for the second region.
     * <p>
     * If the table does not need to be pre-split then return Optional.empty()
     */
    abstract Optional<byte[][]> getRegionSplitKeys();

    /**
     * @return The full human readable name of the table. This name is not used in HBase
     * but can be used in any logging for clarity.
     */
    abstract String getLongName();

    /**
     * @return The abbreviated name of the table that is the actual name of the table used
     * by hbase. The short name should only be a few chars in length
     */
    abstract String getShortName();

    abstract byte[] getShortNameBytes();

    abstract TableName getTableName();

}
