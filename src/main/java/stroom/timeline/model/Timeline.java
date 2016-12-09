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

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.Optional;

public class Timeline {
    //The temporary id until the Timeline is persisted to the TimelineMetaTable
    private static int DETACHED_OBJECT_ID = -1;
    private int id;
    private final String name;
    private final Duration retention;
    private final int saltCount;

    public Timeline(final String name, final Duration retention, final int saltCount) {
        this(DETACHED_OBJECT_ID, name, retention, saltCount);
    }

    public Timeline(final int id, final String name, final Duration retention, final int saltCount) {

        Preconditions.checkArgument(id > 0, "id must be greater than 0");
        Preconditions.checkArgument(name != null && name.length() > 0, "Name must be at least one character in length");
        Preconditions.checkArgument(saltCount > 0, "saltCount must be greater than 0");

        this.id = id;
        this.name = name;
        this.retention = retention != null ? retention : Duration.ZERO;
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
    public Optional<Duration> getRetention() {
        return retention.equals(Duration.ZERO) ? Optional.empty() : Optional.of(retention);
    }

    /**
     * @return The number of different salt values for this timeline
     */
    public int getSaltCount() {
        return saltCount;
    }

    public PersistedState getPersistedState() {
        return id == DETACHED_OBJECT_ID ? PersistedState.DETACHED : PersistedState.PERSISTED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Timeline timeline = (Timeline) o;

        if (id != timeline.id) return false;
        if (saltCount != timeline.saltCount) return false;
        if (!name.equals(timeline.name)) return false;
        return retention.equals(timeline.retention);
    }

    @Override
    public int hashCode() {
        return id;
    }

    public enum PersistedState{
        DETACHED,
        PERSISTED
    }

}
