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
 *
 */

package stroom.timeline.api;

import stroom.timeline.hbase.HBaseConnection;
import stroom.timeline.hbase.HBaseTimelineService;

/**
 * Entry point class for interacting with timelines
 */
public class TimelineServiceFactory {

    public TimelineServiceFactory() {
    }

    /**
     * @param dbConnectionn A provider for the connection object that connects to the underlying database
     * @return A new TimelineService
     */
    public static TimelineService getTimelineService(DBConnection dbConnectionn){

        if (dbConnectionn instanceof HBaseConnection) {
           return new HBaseTimelineService((HBaseConnection)dbConnectionn);
        } else {
            throw new IllegalArgumentException(String.format("Provided DBConnection class %s, is not supported",
                    dbConnectionn.getClass().getName()));
        }
    }
}
