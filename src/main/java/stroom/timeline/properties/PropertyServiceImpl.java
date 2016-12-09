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

import java.util.Properties;

//TODO remove, will be provided by client
@Deprecated
public class PropertyServiceImpl implements PropertyService{

    //TODO need to load these from somewhere
    private final Properties properties = new Properties();

    public PropertyServiceImpl() {
        properties.put(PROP_KEY_ZOOKEEPER_QUORUM,"localhost");
        properties.put(PROP_KEY_ZOOKEEPER_PORT,"2181");
        properties.put(PROP_KEY_ZOOKEEPER_ZNODE_PARENT,"/hbase");
        properties.put(PROP_KEY_HBASE_MASTER_HOST,"localhost");
        properties.put(PROP_KEY_HBASE_MASTER_PORT,"60000");
    }

    @Override
    public String getOrDefaultStringProperty(String key, String defaultValue) {
        return null;
    }

    @Override
    public boolean getBooleanProperty(String key, boolean defaultValue) {
        return false;
    }

    @Override
    public int getBooleanProperty(String key, int defaultValue) {
        return 0;
    }

    @Override
    public long getBooleanProperty(String key, long defaultValue) {
        return 0;
    }
}
