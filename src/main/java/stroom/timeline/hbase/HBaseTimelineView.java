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
import stroom.timeline.hbase.table.TimelineTable;
import stroom.timeline.model.Event;
import stroom.timeline.model.Timeline;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HBaseTimelineView implements TimelineView {

    private final Timeline timeline;
    private final TimelineTable timelineTable;
    private final Duration delay;
    private final Instant offset;
    private final Duration streamTimeout;
    private final Duration topUpRetryInterval;
    private final int fetchSize;
    private final QueueSpliterator<Event> eventQueueSpliterator;
    private final BlockingQueue<Event> eventQueue;

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

    @Override
    public Duration getStreamTimeout() {
        return streamTimeout;
    }

    @Override
    public Duration getTopUpRetryInterval() {
        return topUpRetryInterval;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public Stream<Event> stream(final TimelineView timelineView) {
        return StreamSupport.stream(eventQueueSpliterator,false);
    }

    HBaseTimelineView(final Timeline timeline, final TimelineTable timelineTable,
                      final Duration delay, final Instant offset,
                      final Duration streamTimeout, final Duration topUpRetryInterval,
                      final int fetchSize) {

        this.timeline = timeline;
        this.timelineTable = timelineTable;
        this.delay = delay;
        this.offset = offset;
        this.streamTimeout = streamTimeout;
        this.topUpRetryInterval = topUpRetryInterval;
        this.fetchSize = fetchSize;
        this.eventQueue = new LinkedBlockingQueue<>();

        Supplier<Stream<Event>> eventSupplier = () -> timelineTable.streamEvents(this, fetchSize);

        eventQueueSpliterator = new QueueSpliterator<>(eventQueue, streamTimeout, eventSupplier, topUpRetryInterval);
    }

    @Override
    public String toString() {
        return "HBaseTimelineView{" +
                "timeline=" + timeline +
                ", delay=" + delay +
                ", offset=" + offset +
                ", streamTimeout=" + streamTimeout +
                ", topUpRetryInterval=" + topUpRetryInterval +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
