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

public class MockPropertyService implements PropertyService {

//    Map<String,String> props = new HashMap<>();
    Properties properties = new Properties();

    @Override
    public String getOrDefaultStringProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    @Override
    public boolean getBooleanProperty(String key, boolean defaultValue) {
        String val = properties.getProperty(key);
        return (val != null ? val.toLowerCase().equals("true") : defaultValue);
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
