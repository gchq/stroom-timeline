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
package stroom.timeline.model;

import java.time.Duration;

public class Timeline {
    private final int id;
    private final String name;
    private final Duration retention;
    private final int saltCount;

    public Timeline(final int id, final String name, final Duration retention, final int saltCount) {
        this.id = id;
        this.name = name;
        this.retention = retention;
        this.saltCount = saltCount;
    }

    /**
     * @return The unique identifier for the timeline
     */
    public int getId() {
        return id;
    }

    /**
     * @return The human readable name of the timeline
     */
    public String getName() {
        return name;
    }

    /**
     * @return The time period values are held in the timeline before being purged
     */
    public Duration getRetention() {
        return retention;
    }

    /**
     * @return The number of different salt values for this timeline
     */
    public int getSaltCount() {
        return saltCount;
    }
}
