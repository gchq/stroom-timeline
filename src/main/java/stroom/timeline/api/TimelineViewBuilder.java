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

import java.time.Duration;
import java.time.Instant;

public interface TimelineViewBuilder {

    /**
     * @param delay The delay to set on the TimelineView. A delay of 1 hour
     *              means you will not be able to retrieve events with an
     *              event time more recent than now() minus 1 hour.
     *
     *              If setDelay() is not called no delay will be imposed and all
     *              events can be retrieved.
     */
    TimelineViewBuilder setDelay(Duration delay);

    /**
     * @param offset The point on the timeline to start retrieving events from inclusive.
     *
     *               If setOffset() is not called, the unix epoch is used as the start point
     */
    TimelineViewBuilder setOffset(Instant offset);

    /**
     * Builds the new view
     * @return A new instance of a view over a timeline.
     */
    TimelineView build();
}
