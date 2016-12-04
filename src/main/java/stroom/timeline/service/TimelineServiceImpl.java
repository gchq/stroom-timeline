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
package stroom.timeline.service;

import stroom.timeline.model.Event;
import stroom.timeline.model.Timeline;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class TimelineServiceImpl  implements TimelineService {
    private final String longName;
    private final String shortName;

    //TODO need some way of handling multiple events with the same millisecond time instant
    //if we do nothing then multiple puts to the same row key will result in multiple cell versions.
    //this may be acceptable if the number of events per timestamp is not to great.
    //The alternative is to put each event into a different column on the row, but we need some way of creating
    //a unique col qualifier
//    private final AtomicLong sequenceNo;

    public TimelineServiceImpl(String longName, String shortName) {
        this.longName = longName;
        this.shortName = shortName;

    }

    @Override
    public void putEvent(Timeline timeline, Event event) {
        putEvents(timeline, Arrays.asList(event));
    }

    @Override
    public void putEvents(Timeline timeline, Collection<Event> events) {

    }

    @Override
    public List<Event> take(Timeline timeline, int takeCount) {
        return null;
    }

    @Override
    public List<Event> poll(Timeline timeline, int takeCount) {
        return null;
    }

    @Override
    public Stream<Event> stream(Timeline timeline) {
        return null;
    }

    @Override
    public List<Event> takeRange(Timeline timeline, Instant fromMs, Instant toMs) {
        return null;
    }
}
