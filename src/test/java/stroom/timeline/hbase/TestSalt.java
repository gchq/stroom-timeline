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

package stroom.timeline.hbase;

import org.junit.Assert;
import org.junit.Test;
import stroom.timeline.model.Salt;
import stroom.timeline.model.SaltedRange;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class TestSalt {
    @Test
    public void computeSalt() throws Exception {

        Map<Integer,AtomicInteger> countsMap = new HashMap<>();

        int saltCount = 4;
        Duration saltRangeDuration = Duration.ofMillis(25);
        Salt salt = new Salt(saltCount, saltRangeDuration);

        final LocalDateTime localDateTime = LocalDateTime.of(2016,12,9,0,0,0);

        countsMap.entrySet()
                .stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> entry.getKey() + " - " + entry.getValue().get())
                .forEach(System.out::println);

        IntStream.rangeClosed(1,2000)
                .boxed()
                .map(i -> localDateTime.plus(Duration.ofMillis(i)).toInstant(ZoneOffset.UTC))
                .forEach(newTime -> {
                    short saltVal = salt.computeSalt(newTime);
                    long bucketNo = salt.getTimeBucketNo(newTime);
                    System.out.println(String.format("%s - %s - %s",
                            newTime.toString(),
                            saltVal,
                            bucketNo));
                });

    }

    @Test
    public void getAllSalts() throws Exception {

    }

    @Test
    public void getSaltedRanges() throws Exception{
        int saltCount = 4;
        Duration saltRangeDuration = Duration.ofSeconds(1);
        Salt salt = new Salt(saltCount, saltRangeDuration);
        List<SaltedRange> ranges = salt.getSaltedRanges(Instant.now());

        Assert.assertEquals(saltCount, ranges.size());
        Instant startOfFirstRange = ranges.get(0).getRangeStartInc();
        Instant endOfLastRange = ranges.get(saltCount - 1).getRangeEndExcl();

        Assert.assertEquals((saltRangeDuration.toMillis() * saltCount) , (endOfLastRange.toEpochMilli() - startOfFirstRange.toEpochMilli()));

        long uniqueSaltsCount = ranges.stream()
                .map(SaltedRange::getSalt)
                .distinct()
                .count();

        Assert.assertEquals(saltCount, uniqueSaltsCount);


        ranges.stream().forEach(System.out::println);

    }

}