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

import stroom.timeline.model.Timeline;

import java.util.HashMap;
import java.util.Map;

public class TimelineFactoryImpl implements TimelineFactory {

    private final Map<Timeline, TimelineTable> timelineTables = new HashMap<>();

    public TimelineFactoryImpl() {
        //read the meta table to build a TimelineTable for each entry and put it in the map
    }

    @Override
    public TimelineTable getTimeline(Timeline timeline) {
        TimelineTable timelineTable = timelineTables.get(timeline);
        if (timelineTable == null) {
            throw new RuntimeException("No timeline exists for name:" + timeline);
        }
        return timelineTable;
    }

    @Override
    public TimelineTable getOrCreateTimeline(Timeline timeline) {
        TimelineTable timelineTable = timelineTables.get(timeline);
        if (timelineTable == null) {
            timelineTable = createTimelineTable(timeline);
        }
        return timelineTable;
    }

    private TimelineTable createTimelineTable(Timeline timeline){
        //TODO create a new table for this timeline and add an entry to the timelines table,
        //then create a new service and add it to the map, then return it.
        //May need some locking around this
       return  null;
    }
}
