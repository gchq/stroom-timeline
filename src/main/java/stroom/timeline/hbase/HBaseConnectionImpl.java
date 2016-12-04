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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import stroom.timeline.properties.PropertyService;

import java.io.IOException;

public class HBaseConnectionImpl implements HBaseConnection {
    private final PropertyService propertyService;
    Connection connection;

    public HBaseConnectionImpl(PropertyService propertyService) {
        this.propertyService = propertyService;

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
        conf.set("hbase.zookeeper.property.clientPort",
                propertyService.getOrDefaultStringProperty(PropertyService.PROP_KEY_ZOOKEEPER_PORT,"2181"));
        conf.set("hbase.zookeeper.quorum",
                propertyService.getOrDefaultStringProperty(PropertyService.PROP_KEY_ZOOKEEPER_QUORUM,"localhost"));
        conf.set("zookeeper.znode.parent",
                propertyService.getOrDefaultStringProperty(PropertyService.PROP_KEY_ZOOKEEPER_ZNODE_PARENT,"/hbase"));
        String hbaseMasterHost = propertyService.getOrDefaultStringProperty(PropertyService.PROP_KEY_HBASE_MASTER_HOST,"localhost");
        String hbaseMasterPort = propertyService.getOrDefaultStringProperty(PropertyService.PROP_KEY_HBASE_MASTER_PORT,"60000");
        conf.set("hbase.master", hbaseMasterHost + ":" + hbaseMasterPort);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create HBase connection", e);
        }
        this.connection = connection;

    }



}
