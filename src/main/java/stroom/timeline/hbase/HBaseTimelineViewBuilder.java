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

import com.google.common.base.Preconditions;
import stroom.timeline.api.TimelineView;
import stroom.timeline.api.TimelineViewBuilder;
import stroom.timeline.hbase.table.TimelineTable;
import stroom.timeline.model.Timeline;

import java.time.Duration;
import java.time.Instant;

public class HBaseTimelineViewBuilder implements TimelineViewBuilder {
    private final Timeline timeline;
    private final TimelineTable timelineTable;
    private Duration delay = Duration.ZERO;
    private Instant offset = Instant.EPOCH;
    private Duration streamTimout = Duration.ofMillis(Long.MAX_VALUE);
    private Duration topUpRetryDelay = Duration.ofSeconds(1);
    private int fetchSize = 1000;

    public HBaseTimelineViewBuilder(final Timeline timeline, final TimelineTable timelineTable) {
        Preconditions.checkNotNull(timeline);
        Preconditions.checkNotNull(timelineTable);
        this.timeline = timeline;
        this.timelineTable = timelineTable;
    }

    @Override
    public HBaseTimelineViewBuilder setDelay(Duration delay) {
        Preconditions.checkNotNull(delay);
        this.delay = delay;
        return this;
    }

    @Override
    public HBaseTimelineViewBuilder setOffset(Instant offset) {
        Preconditions.checkNotNull(offset);
        this.offset = offset;
        return this;
    }

    @Override
    public TimelineViewBuilder setStreamTimeout(final Duration streamTimeout) {
        Preconditions.checkNotNull(streamTimeout);
        this.streamTimout = streamTimeout;
        return this;
    }

    @Override
    public TimelineViewBuilder setTopUpRetryDelay(final Duration topUpRetryDelay) {
        Preconditions.checkNotNull(topUpRetryDelay);
        this.topUpRetryDelay = topUpRetryDelay;
        return this;
    }

    @Override
    public TimelineViewBuilder setFetchSize(final int rowCount) {
        Preconditions.checkArgument(rowCount >= 1, "rowCount must be >= 1");
        this.fetchSize = rowCount;
        return this;
    }

    @Override
    public TimelineView build() {
        return new HBaseTimelineView(timeline, timelineTable, delay, offset, streamTimout, topUpRetryDelay, fetchSize);
    }
}
