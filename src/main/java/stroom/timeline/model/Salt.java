
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

package stroom.timeline.model;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Salt {


    private static final Logger logger = Logger.getLogger(Salt.class);

    private final int saltCount;
    private final short[] allSaltValues;
    private final Duration saltRangeDuration;

    private static final short SINGLE_SALT_VALUE = 0;

    //TODO needs to be defined in a property
    //This is the maximum length of time we could have contiguous events for
//    private static final Duration saltRangeDuration = Duration.ofMillis(250);

    //TODO either the salt's short values need to match the order
    //of the time buckets they represent or we need a comparator to
    //do that for us. This is because we need to be able to group the returned
    //rows by salt and then quickly put each group into order without having to order
    //by the underlying event times


    public Salt(final int saltCount, final Duration saltRangeDuration) {
        Preconditions.checkArgument(saltCount > 0);
        Preconditions.checkNotNull(saltRangeDuration);

        this.saltCount = saltCount;
        this.allSaltValues = buildAllSalts();
        //duration is meaningless with one salt value
        this.saltRangeDuration = saltCount == 1 ? Duration.ZERO : saltRangeDuration;
    }

    public short computeSalt(final Instant time) {

        if (saltCount > 1) {
            final long bucketNo = getTimeBucketNo(time);
            return (short) (bucketNo % saltCount);
        } else {
            return SINGLE_SALT_VALUE;
        }
    }

    /**
     * Divide up the passed time into buckets of size saltRangeDuration and return
     * which bucket the time is in
     *
     * @param time
     * @return
     */
    public  long getTimeBucketNo(final Instant time) {
        if (saltCount > 1) {
            return time.toEpochMilli() / saltRangeDuration.toMillis();
        } else {
            //timeBucketNo has no real meaning with one salt value as we have one infinite timebucket
            return 0L;
        }
    }

    private short[] buildAllSalts() {
        final short[] salts = new short[saltCount];

        for (int i = 0; i < saltCount; i++) {
            salts[i] = (short) i;
        }
        return salts;
    }

    public  short nextSalt(final short currentSalt) {
        short salt = currentSalt;
        return (++salt == saltCount) ? 0 : salt;
    }

    /**
     * Starting from the salted range of the start time build the set of contiguous salted ranges until
     * there is one for each salt value
     */
    @Deprecated //not being used anywhere
    public List<SaltedRange> getSaltedRanges(final Instant startTime) {
        Preconditions.checkArgument(saltCount > 1, "No concept of saltedRanges for a single salt value");
        Preconditions.checkNotNull(startTime);
        Preconditions.checkNotNull(saltRangeDuration);

        final List<SaltedRange> ranges = new ArrayList<>();
        short firstSalt = computeSalt(startTime);
        short currentSalt = firstSalt;
        Instant workingTime = startTime;

        do {
            final long timeBucketNo = getTimeBucketNo(workingTime);
            Instant rangeStartInc = Instant.ofEpochMilli(timeBucketNo * saltRangeDuration.toMillis());
            Instant rangeEndExcl = rangeStartInc.plus(saltRangeDuration);
            ranges.add(new SaltedRange(currentSalt, rangeStartInc, rangeEndExcl));
            workingTime = rangeEndExcl;
            currentSalt = nextSalt(currentSalt);
        } while (currentSalt != firstSalt);

        return ranges;
    }

    /**
     * @return The number of different salt values for this timeline
     */
    public int getSaltCount() {
        return saltCount;
    }

    /**
     * @return All possible salt values
     */
    public short[] getAllSaltValues() {
        return allSaltValues;
    }

    /**
     * @return This is the maximum length of time we could have contiguous events for
     */
    public Duration getSaltRangeDuration() {
        return saltRangeDuration;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Salt salt = (Salt) o;

        if (saltCount != salt.saltCount) return false;
        return saltRangeDuration.equals(salt.saltRangeDuration);
    }

    @Override
    public int hashCode() {
        int result = saltCount;
        result = 31 * result + saltRangeDuration.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Salt{" +
                "saltCount=" + saltCount +
                ", allSaltValues=" + Arrays.toString(allSaltValues) +
                ", saltRangeDuration=" + saltRangeDuration +
                '}';
    }
}
