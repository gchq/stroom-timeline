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
import org.junit.Assert;
import org.junit.Test;
import stroom.timeline.test.AbstractTest;

import java.nio.ByteBuffer;

public class TestMultiPartIdentifier extends AbstractTest {

    String strVal = "MyString";
    Long longVal = 123456789L;
    Integer intVal = 321;
    byte[] bStrVal = Bytes.toBytes(strVal);
    byte[] bLongVal = Bytes.toBytes(longVal);
    byte[] bIntVal = Bytes.toBytes(intVal);
    int totalByteLength = bStrVal.length + bLongVal.length + bIntVal.length;



    @Test
    public void getBytes() throws Exception {
        MultiPartIdentifier id = buildId();

        byte[] expectedBytes = ByteBuffer.allocate(totalByteLength)
                .put(bStrVal)
                .put(bLongVal)
                .put(bIntVal)
                .array();
        Assert.assertArrayEquals(expectedBytes, id.getBytes());
    }

    @Test
    public void toHumanReadable() throws Exception {
        MultiPartIdentifier id = buildId();
        String expectedStr = strVal + ":" + longVal + ":" + intVal;
        Assert.assertEquals(expectedStr, id.toHumanReadable());
    }

    @Test
    public void getValue() throws Exception {
        Object[] values = buildId().getValue();
        Assert.assertEquals(3, values.length);
        Assert.assertEquals(strVal, values[0]);
        Assert.assertEquals(longVal, values[1]);
        Assert.assertEquals(intVal, values[2]);
    }

    @Test
    public void getBytes_single() throws Exception {
        MultiPartIdentifier id = buildId_single();

        byte[] expectedBytes = bStrVal;
        Assert.assertArrayEquals(expectedBytes, id.getBytes());
    }

    @Test
    public void toHumanReadable_single() throws Exception {
        MultiPartIdentifier id = buildId_single();
        String expectedStr = strVal;
        Assert.assertEquals(expectedStr, id.toHumanReadable());
    }

    @Test
    public void getValue_single() throws Exception {
        Object[] values = buildId_single().getValue();
        Assert.assertEquals(1, values.length);
        Assert.assertEquals(strVal, values[0]);
    }

    private MultiPartIdentifier buildId(){
        return new MultiPartIdentifier(strVal, longVal, intVal);
    }

    private MultiPartIdentifier buildId_single(){
        return new MultiPartIdentifier(strVal);
    }
}