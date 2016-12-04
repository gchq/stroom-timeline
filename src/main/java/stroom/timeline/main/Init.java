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
package stroom.timeline.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class Init {
//    public static final Sring HOST = "172.17.0.2";
    public static final String HOST = "localhost";

    public static void main(String[] args) throws IOException {
       new Init().run();
    }


    private void run() throws IOException {
        TableName tableName = TableName.valueOf("stock-prices");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", HOST);
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.set("hbase.master", HOST + ":60000");

        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("cf")));
        }

        Table table = conn.getTable(tableName);
        Put p = new Put(Bytes.toBytes("AAPL10232015"));
        p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("close"), Bytes.toBytes(119));
        table.put(p);

        Result r = table.get(new Get(Bytes.toBytes("AAPL10232015")));
        System.out.println(r);
    }

}
