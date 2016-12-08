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
package stroom.timeline.hbase.structure;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Row key structure:
 * [salt][event time as ms since epoch]
 * Row key types:
 * [short][long]
 * Row key byte widths:
 * [2][8]
 */
public class RowKey {
    public static int SALT_LENGTH = 2;
    public static int EVENT_TIME_LENGTH = 8;
    public static int ROW_KEY_LENGTH = SALT_LENGTH + EVENT_TIME_LENGTH;
    private final byte[] bRowKey;

//    public RowKey(short salt, long eventTime) {
//        byte[] key = new byte[ROW_KEY_LENGTH];
//        this.bRowKey = ByteBuffer.wrap(key).put(Bytes.toBytes(salt)).put(Bytes.toBytes(eventTime)).array();
//    }

    public RowKey(byte[] bRowKey){
        this.bRowKey = bRowKey;
    }

    public RowKey(final byte[] salt, byte[] eventTime){
        byte[] key = new byte[ROW_KEY_LENGTH];
        this.bRowKey = ByteBuffer.wrap(key).put(salt).put(eventTime).array();
    }

    public byte[] getRowKeyBytes() {
        return bRowKey;
    }

    public byte[] getSaltBytes(){
        return Arrays.copyOfRange(bRowKey, 0,SALT_LENGTH);
    }

    public byte[] getEventTimeBytes(){
        return Arrays.copyOfRange(bRowKey, SALT_LENGTH,ROW_KEY_LENGTH);
    }

}
