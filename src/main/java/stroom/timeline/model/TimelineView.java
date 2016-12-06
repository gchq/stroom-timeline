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
package stroom.timeline.model;

import java.time.Duration;
import java.time.Instant;

public class TimelineView {

    private final Timeline timeline;
    private final Duration delay;
    private final Instant offset;


    public TimelineViewBuilder builder(Timeline timeline) {
        return new TimelineViewBuilder(timeline);
    }

    public Timeline getTimeline() {
        return timeline;
    }

    public Duration getDelay() {
        return delay;
    }

    public Instant getOffset() {
        return offset;
    }

    private TimelineView(Timeline timeline, Duration delay, Instant offset) {
        this.timeline = timeline;
        this.delay = delay;
        this.offset = offset;
    }

    public static class TimelineViewBuilder {
        private Timeline timeline;
        private Duration delay = Duration.ZERO;
        private Instant offset = Instant.EPOCH;

        public TimelineViewBuilder(Timeline timeline) {
            this.timeline = timeline;
        }

        public TimelineViewBuilder setDelay(Duration delay) {
            this.delay = delay;
            return this;
        }

        public TimelineViewBuilder setOffset(Instant offset) {
            this.offset = offset;
            return this;
        }

        public TimelineView build() {
            return new TimelineView(timeline, delay, offset);
        }
    }
}
