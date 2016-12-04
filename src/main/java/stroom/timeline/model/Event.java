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

import java.time.Instant;
import java.util.UUID;

public class Event {
    private final Instant eventTime;
    private byte[] content;
    //provides uniqueness in case of a clash on time (which is quite likely)
    private final UUID uuid;


    public Event(Instant eventTime, byte[] content, UUID uuid) {
        this.eventTime = eventTime;
        this.content = content;
        this.uuid = uuid;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public byte[] getContent() {
        return content;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventTime=" + eventTime +
                ", uuid=" + uuid +
                '}';
    }
}
