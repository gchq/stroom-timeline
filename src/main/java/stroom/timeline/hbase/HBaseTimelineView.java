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

import stroom.timeline.api.TimelineView;
import stroom.timeline.model.Event;
import stroom.timeline.model.Timeline;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public class HBaseTimelineView implements TimelineView {

    private final Timeline timeline;
    private final Duration delay;
    private final Instant offset;

    //TODO create a queue so clients can grab events off the top and it can be refilled from the bottom
    //by calls to HBase. We can then prefetch some events to keep the queue stocked up removing any latency
    //between the call to take/poll and getting the results.


    public static HBaseTimelineViewBuilder builder(Timeline timeline) {
        return new HBaseTimelineViewBuilder(timeline);
    }

    @Override
    public Timeline getTimeline() {
        return timeline;
    }

    @Override
    public Duration getDelay() {
        return delay;
    }

    @Override
    public Instant getOffset() {
        return offset;
    }

    HBaseTimelineView(Timeline timeline, Duration delay, Instant offset) {
        this.timeline = timeline;
        this.delay = delay;
        this.offset = offset;
    }

    @Override
    public List<Event> take(TimelineView timelineView, int takeCount) {
        return null;
    }

    @Override
    public List<Event> poll(TimelineView timelineView, int takeCount) {
        return null;
    }

    @Override
    public Stream<Event> stream(TimelineView timelineView) {
        return null;
    }

    @Override
    public List<Event> takeRange(TimelineView timelineView, Instant fromOffset, Instant toOffset) {
        return null;
    }

    @Override
    public String toString() {
        return "HBaseTimelineView{" +
                "timeline=" + timeline +
                ", delay=" + delay +
                ", offset=" + offset +
                '}';
    }
}
