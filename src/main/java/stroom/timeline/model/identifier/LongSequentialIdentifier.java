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

package stroom.timeline.model.identifier;

import org.apache.hadoop.hbase.util.Bytes;

public class LongSequentialIdentifier implements SequentialIdentifierProvider {

    private final long id;

    public LongSequentialIdentifier(long id) {
        this.id = id;
    }

    @Override
    public byte[] getBytes() {
        return Bytes.toBytes(id);
    }

    @Override
    public String toHumanReadable() {
        return Long.toString(id);
    }

    @Override
    public Long getValue() {
        return id;
    }
}
