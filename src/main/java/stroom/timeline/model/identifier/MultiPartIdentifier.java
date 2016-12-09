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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Class for representing a complex multi-part identifier such as wehen you
 * have a compound key. If the parts are hierarchical in nature, e.g.
 * BatchNo:IdInBatch then they should be provided to the constructor in the
 * order outer -> inner to ensure correct ordering in the Timeline
 */
public class MultiPartIdentifier implements SequentialIdentifierProvider<Object[]> {

    private final Object[] values;
    private final byte[] bValues;

    public MultiPartIdentifier(Object... values) {
        if (values == null || values.length == 0){
            String valuesStr = values == null ? "NULL" : toHumanReadable();
            throw new IllegalArgumentException(String.format("values %s must contain at least one element", valuesStr));
        }
        this.values = values;
        this.bValues = buildBytes();
    }

    private byte[] buildBytes(){
        byte[][] byteArrayChunks = new byte[values.length][];

        int i = 0;
        int len = 0;
        for (Object value : values){
            byte[] chunk;
            if (value instanceof String){
                chunk = Bytes.toBytes(String.class.cast(value));
            } else if (value instanceof Integer){
                chunk = Bytes.toBytes(Integer.class.cast(value));
            } else if (value instanceof Long){
                chunk = Bytes.toBytes(Long.class.cast(value));
            } else if (value instanceof Double){
                chunk = Bytes.toBytes(Double.class.cast(value));
            } else if (value instanceof BigDecimal) {
                chunk = Bytes.toBytes(BigDecimal.class.cast(value));
            } else {
                throw new IllegalArgumentException(String.format("The type %s for value %s is not supported", value.getClass().getName(), value));
            }
            byteArrayChunks[i++] = chunk;
            len += chunk.length;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(len);

        for (byte[] chunk : byteArrayChunks){
            byteBuffer.put(chunk);
        }
        return byteBuffer.array();
    }

    @Override
    public byte[] getBytes() {
        return bValues;
    }

    @Override
    public String toHumanReadable() {
        return StreamSupport.stream(Arrays.spliterator(values),false)
                .map(val -> val.toString())
                .collect(Collectors.joining(":"));
    }

    @Override
    public Object[] getValue() {
        return values;
    }

    @Override
    public String toString() {
        return toHumanReadable();
    }
}
