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

import java.time.Instant;

public class SaltedRange implements Comparable<SaltedRange>{
    final short salt;
    final Instant rangeStartInc;
    final Instant rangeEndExcl;

    public SaltedRange(final short salt, final Instant rangeStartInc, final Instant rangeEndExcl) {
        this.salt = salt;
        this.rangeStartInc = rangeStartInc;
        this.rangeEndExcl = rangeEndExcl;
    }

    public short getSalt() {
        return salt;
    }

    public Instant getRangeStartInc() {
        return rangeStartInc;
    }

    public Instant getRangeEndExcl() {
        return rangeEndExcl;
    }

    @Override
    public String toString() {
        return "SaltedRange{" +
                "salt=" + salt +
                ", rangeStartInc=" + rangeStartInc +
                ", rangeEndExcl=" + rangeEndExcl +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final SaltedRange that = (SaltedRange) o;

        if (salt != that.salt) return false;
        if (!rangeStartInc.equals(that.rangeStartInc)) return false;
        return rangeEndExcl.equals(that.rangeEndExcl);
    }

    @Override
    public int hashCode() {
        int result = (int) salt;
        result = 31 * result + rangeStartInc.hashCode();
        result = 31 * result + rangeEndExcl.hashCode();
        return result;
    }


    @Override
    public int compareTo(final SaltedRange o) {
       return this.rangeStartInc.compareTo(o.getRangeStartInc());
    }
}
