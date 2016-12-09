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

public abstract class AbstractTable {

    private final HBaseConnection hBaseConnection;

    public AbstractTable(HBaseConnection hBaseConnection) {
        Preconditions.checkNotNull(hBaseConnection);

        this.hBaseConnection = hBaseConnection;
    }

    void initialiseTable(){
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
                admin.createTable(tableDescriptor);
            } catch (IOException e) {
                throw new RuntimeException("Error creating table " + getLongName(), e);
            }
        }


    }

    Table getTable() throws IOException {
        return hBaseConnection.getConnection().getTable(getTableName());
    }

    abstract HTableDescriptor getTableDesctptor();
    abstract String getLongName();
    abstract String getShortName();
    abstract byte[] getShortNameBytes();
    abstract TableName getTableName();

}
