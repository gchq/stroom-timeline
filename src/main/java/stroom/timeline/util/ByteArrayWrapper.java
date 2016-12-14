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

package stroom.timeline.util;

import java.util.Arrays;

public class ByteArrayWrapper {

    private final byte[] value;

    private ByteArrayWrapper(final byte[] value) {
        this.value = value;
    }

    public static ByteArrayWrapper fromBytes(byte[] value) {
        return new ByteArrayWrapper(value);
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ByteArrayWrapper that = (ByteArrayWrapper) o;

        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "ByteArrayWrapper{" +
                "value=" + ByteArrayUtils.byteArrayToAllForms(value) +
                '}';
    }
}