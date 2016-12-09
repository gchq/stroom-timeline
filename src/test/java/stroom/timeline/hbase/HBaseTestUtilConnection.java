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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import stroom.timeline.properties.PropertyService;

import java.io.IOException;

public class HBaseTestUtilConnection implements HBaseConnection {

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

    @Override
    public PropertyService getPropertyService() {
        throw new UnsupportedOperationException("Method not supported in this implementation");
    }

    public void setup() {
//        Configuration altConfig = HBaseConfiguration.create();
//        final int nonDefaultMasterInfoPort = 3333;
//        final int nonDefaultRegionServerPort = 4444;
//        altConfig.setInt(HConstants.MASTER_INFO_PORT, nonDefaultMasterInfoPort);
//        altConfig.setInt(HConstants.REGIONSERVER_PORT, nonDefaultRegionServerPort);
//        hBaseTestingUtility = new HBaseTestingUtility(altConfig);
        hBaseTestingUtility = new HBaseTestingUtility();
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

}
