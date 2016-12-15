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
package stroom.timeline.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.StreamSupport;

public class HBaseTestingUtilConnection implements HBaseConnection {

    private HBaseTestingUtility hBaseTestingUtility;

    @Override
    public Connection getConnection() {
        try {
            return hBaseTestingUtility.getConnection();
        } catch (IOException e) {
            throw new RuntimeException("Error getting connection to mini cluster", e);
        }
    }

    @Override
    public Admin getAdmin() {
        try {
            return hBaseTestingUtility.getHBaseAdmin();
        } catch (IOException e) {
            throw new RuntimeException("Error getting HBaseAdmin", e);
        }
    }

    public void setup() {
        hBaseTestingUtility = new HBaseTestingUtility();
        System.out.println("Supported compression algorithms: " + Arrays.toString(hBaseTestingUtility.getSupportedCompressionAlgorithms()));

        try {
            hBaseTestingUtility.startMiniCluster();
        } catch (Exception e) {
            throw new RuntimeException("Error starting up mini cluster", e);
        }
    }

    public void teardown() {
        try {
            hBaseTestingUtility.shutdownMiniCluster();
            hBaseTestingUtility = null;
        } catch (Exception e) {
            throw new RuntimeException("Error shutting down mini cluster", e);
        }
    }

    public HBaseTestingUtility gethBaseTestingUtility() {
        return hBaseTestingUtility;
    }

    public long getCellCount(String tableName, byte[] family) throws IOException {
       Scan scan = new Scan().addFamily(family);
        TableName tableNameObj = TableName.valueOf(tableName);

        try (final Table table = getConnection().getTable(tableNameObj)){
            ResultScanner resultScanner = table.getScanner(scan);

            return StreamSupport.stream(resultScanner.spliterator(), true)
                    .mapToLong(result -> result.listCells().size())
                    .sum();
        }
    }

}
