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
package stroom.timeline.properties;

public interface PropertyService {

    String getOrDefaultStringProperty(String key, String defaultValue);

    boolean getBooleanProperty(String key, boolean defaultValue);

    int getBooleanProperty(String key, int defaultValue);

    long getBooleanProperty(String key, long defaultValue);

    String PROP_KEY_ZOOKEEPER_QUORUM = "stroom.timeline.hbase.zookeeper.quorum";
    String PROP_KEY_ZOOKEEPER_PORT = "stroom.timeline.hbase.zookeeper.port";
    String PROP_KEY_ZOOKEEPER_ZNODE_PARENT = "stroom.timeline.hbase.zookeeper.znodeParent";
    String PROP_KEY_HBASE_MASTER_HOST = "stroom.timeline.hbase.master.host";
    String PROP_KEY_HBASE_MASTER_PORT = "stroom.timeline.hbase.master.port";
}
