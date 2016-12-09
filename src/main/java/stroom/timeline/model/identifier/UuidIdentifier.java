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

package stroom.timeline.model.identifier;/*
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

import stroom.timeline.util.UuidAdapter;

import java.util.UUID;

/**
 * This is obviously not sequential in nature but is used as a fall back when a globally unique
 * sequential identifier cannot be provided.  As UUIDs have no order this means events with
 * the same time stamp will be stored in a random order. This may be an acceptable limitation
 */
public class UuidIdentifier implements SequentialIdentifierProvider<UUID> {

    private final UUID uuid;

    public UuidIdentifier() {
        uuid = UUID.randomUUID();
    }

    @Override
    public byte[] getBytes() {
        return UuidAdapter.getBytesFromUUID(uuid);
    }

    @Override
    public String toHumanReadable() {
        return uuid.toString();
    }

    @Override
    public UUID getValue() {
        return uuid;
    }
}
