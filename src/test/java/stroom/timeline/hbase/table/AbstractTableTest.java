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
package stroom.timeline.hbase.table;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.timeline.hbase.HBaseTestingUtilConnection;
import stroom.timeline.test.AbstractTest;

public class AbstractTableTest extends AbstractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableTest.class);

    HBaseTestingUtilConnection hBaseTestUtilConnection = new HBaseTestingUtilConnection();

    @Before
    public void setUp() throws Exception {
        LOGGER.info("Setting up HBaseTestingUtilConnection");
        hBaseTestUtilConnection.setup();
    }

    @After
    public void tearDown() throws Exception {
        LOGGER.info("Tearing down HBaseTestingUtilConnection");
        hBaseTestUtilConnection.teardown();
    }
}
