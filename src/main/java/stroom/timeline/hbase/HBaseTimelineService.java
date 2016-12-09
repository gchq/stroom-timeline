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
package stroom.timeline.hbase;

import stroom.timeline.api.TimelineService;
import stroom.timeline.api.TimelineViewBuilder;
import stroom.timeline.hbase.table.TimelineMetaTable;
import stroom.timeline.hbase.table.TimelineTable;
import stroom.timeline.model.OrderedEvent;
import stroom.timeline.model.Timeline;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HBaseTimelineService implements TimelineService {

    //TODO just temporary until the timelineMeta table is implemented
    private static final Timeline HARD_CODED_TIMELINE = new Timeline(0,"HardCodedTimeLine", Duration.ofDays(30), 1);

    private final HBaseConnection hBaseConnection;

    private final TimelineMetaTable timelineMetaTable;

    //One TimelineTable per TimeLine in the TimelineMeta table so cache them all here
    private final ConcurrentMap<Timeline, TimelineTable> timelineTablesMap = new ConcurrentHashMap<>();

    public HBaseTimelineService(HBaseConnection hBaseConnection) {
        this.hBaseConnection = hBaseConnection;
        this.timelineMetaTable = new TimelineMetaTable(hBaseConnection);
    }

    @Override
    public Optional<Timeline> fetchTimeline(String name) {
        //TODO replace with a table lookup
        return Optional.of(HARD_CODED_TIMELINE);
    }

    @Override
    public Optional<Timeline> fetchTimeline(int id) {
        //TODO replace with a table lookup
        return Optional.of(HARD_CODED_TIMELINE);
    }

    @Override
    public List<Timeline> fetchAllTimelines() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Timeline saveTimeline(Timeline timeline) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void putEvent(Timeline timeline, OrderedEvent event) {
        putEvents(timeline, Arrays.asList(event));
    }

    @Override
    public void putEvents(Timeline timeline, Collection<OrderedEvent> events) {
        TimelineTable timelineTable = getTimelineTable(timeline);
        timelineTable.putEvents(events);
    }

    @Override
    public TimelineViewBuilder getTimelineViewBuilder(Timeline timeline) {
        return new HBaseTimelineViewBuilder(timeline);
    }

    private TimelineTable getTimelineTable(Timeline timeline) {
        if (timeline.getPersistedState().equals(Timeline.PersistedState.DETACHED)) {
            throw new RuntimeException(String.format("Timeline %s is not yet persisted to the TimelineMeta table", timeline.toString()));
        }

        return timelineTablesMap.computeIfAbsent(timeline, (key) -> new TimelineTable(key, hBaseConnection));
    }

}
