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
import stroom.timeline.model.Salt;
import stroom.timeline.model.Timeline;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This represents a view onto a Timeline, typically starting from a point somewhere
 * along the timeline. The view also support imposing a delay to preventevents  more recent
 * than a set duration from being retrieved. This delay is relative to the system time (now()).
 *
 * Multiple TimelineView instances can be created against the same Timeline. This class is intended
 * for use by a single thread, given the sequential nature of the data it returns.
 *
 * It will hold state, to keep track of the current offset as it scans along the timeline.
 *
 * Currently scanning along the timeline is only supported in a forward direction.
 *
 * If you need to change the offset simply discard this timelineView instance and create a new one
 * with the desired offset.
 */
public interface TimelineView {

    /**
     * @return The underlying Timeline that this TimelineView is scanning
     */
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


    /**
     * @return The unique identifier for the timeline
     */
    public default int getId() {
        return getTimeline().getId();
    }

    /**
     * @return The human readable name of the timeline
     */
    public default String getName() {
        return getTimeline().getName();
    }

    /**
     * @return The time period values are held in the timeline before being purged
     */
    public default Optional<Duration> getRetention() {
        return getTimeline().getRetention();
    }

    public default Salt getSalt() {
        return getTimeline().getSalt();
    }


}
