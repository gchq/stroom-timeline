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

package stroom.timeline.hbase.structure;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.timeline.test.AbstractTest;
import stroom.timeline.util.ByteArrayUtils;

import java.time.Instant;
import java.util.Arrays;

public class TestRowKey extends AbstractTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestRowKey.class);

    @Test
    public void getRowKeyBytes() throws Exception {

        short salt = 0;
        byte[] bSalt = Bytes.toBytes(salt);

        Assert.assertEquals(2, bSalt.length);

        byte[] bTime = Bytes.toBytes(Instant.now().toEpochMilli());
        RowKey rowKey = new RowKey(bSalt, bTime);

        byte[]bRowKey = rowKey.getRowKeyBytes();

        LOGGER.info("bRowKey: {}", ByteArrayUtils.byteArrayToAllForms(rowKey.getRowKeyBytes()));
        LOGGER.info("bSalt: {}", ByteArrayUtils.byteArrayToAllForms(bSalt));
        LOGGER.info("bTime: {}", ByteArrayUtils.byteArrayToAllForms(bTime));

        Assert.assertArrayEquals(bSalt, Arrays.copyOf(bRowKey, bSalt.length));
        Assert.assertArrayEquals(bTime, Arrays.copyOfRange(bRowKey, bSalt.length, bRowKey.length));
    }

    @Test
    public void getSaltBytes() throws Exception {
        short salt = 0;
        byte[] bSalt = Bytes.toBytes(salt);

        byte[] bTime = Bytes.toBytes(Instant.now().toEpochMilli());
        RowKey rowKey = new RowKey(bSalt, bTime);

        Assert.assertArrayEquals(bSalt, rowKey.getSaltBytes());
    }

    @Test
    public void getEventTimeBytes() throws Exception {
        short salt = 0;
        byte[] bSalt = Bytes.toBytes(salt);

        byte[] bTime = Bytes.toBytes(Instant.now().toEpochMilli());
        RowKey rowKey = new RowKey(bSalt, bTime);

        Assert.assertArrayEquals(bTime, rowKey.getEventTimeBytes());
    }

}