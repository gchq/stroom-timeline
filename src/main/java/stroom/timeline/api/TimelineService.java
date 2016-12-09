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
package stroom.timeline.api;

import stroom.timeline.model.Event;
import stroom.timeline.model.Timeline;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface TimelineService {


    Optional<Timeline> fetchTimeline(final String name);

    Optional<Timeline> fetchTimeline(final int id);

    List<Timeline> fetchAllTimelines();

    Timeline saveTimeline(final Timeline timeline);

    /**
     * Put a single event into the specified timeline. Events can be added in any order.
     * @param timeline
     * @param event
     */
    void putEvent(Timeline timeline, Event event);

    /**
     * Put a collection of events into the specified timeline. Events can be added in any order.
     * @param timeline
     * @param events
     */
    void putEvents(Timeline timeline, Collection<Event> events);

    /**
     * @param timeline The Timeline instance to create the view on
     * @return A builder object for creating a new @TimelineView instance
     */
    TimelineViewBuilder getTimelineViewBuilder(Timeline timeline);

}
