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

import stroom.timeline.model.Event;
import stroom.timeline.model.Timeline;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public interface TimelineView {
    Timeline getTimeline();

    Duration getDelay();

    Instant getOffset();

    //TODO should the builder for the TimelineView be in here?

    //TODO should take/poll/stream/takeRange/etc be on the TimelineView as opposed to here
    //as that will probably hold some form of internal queue with the data in it
    List<Event> take(TimelineView timelineView, int takeCount);
    List<Event> poll(TimelineView timelineView, int takeCount);

    /**
     * Return an infinite stream of ordered events, will block if no events are available
     */
    Stream<Event> stream(TimelineView timelineView);

    List<Event> takeRange(TimelineView timelineView, Instant fromOffset, Instant toOffset);
}
