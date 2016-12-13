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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import stroom.timeline.properties.PropertyService;

import java.io.IOException;

public class HBaseConnectionImpl implements HBaseConnection {
    private final int zkClientPort;
    private final String zkQuorum;
    private final String zkParent;
    private final int hbasePort;
    private final String hbaseHost;
    Connection connection;

    public HBaseConnectionImpl(
            final int zkClientPort,
            final String zkQuorum,
            final String zkParent,
            final String hbaseHost,
            final int hbasePort) {

        this.zkClientPort = zkClientPort;
        this.zkQuorum = zkQuorum;
        this.zkParent = zkParent;
        this.hbaseHost = hbaseHost;
        this.hbasePort = hbasePort;

        Preconditions.checkArgument(zkClientPort != 0);
        Preconditions.checkNotNull(zkQuorum);
        Preconditions.checkNotNull(zkParent);
        Preconditions.checkArgument(hbasePort != 0);
        Preconditions.checkNotNull(hbaseHost);

        //eagerly create the connection
        createConnection();
    }

    @Override
    public Connection getConnection(){
        //TODO need to handle it not being there or being closed.
       return connection;
    }

    @Override
    public Admin getAdmin(){
        Admin admin = null;
        try {
            admin = getConnection().getAdmin();
        } catch (IOException e) {
            throw new RuntimeException("Error getting admin for connection", e);
        }
        return admin;
    }

    private void createConnection(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(zkClientPort));
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("zookeeper.znode.parent",zkParent);
        conf.set("hbase.master", String.format("%s:%s", hbaseHost, hbasePort));

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create HBase connection", e);
        }
    }



}
