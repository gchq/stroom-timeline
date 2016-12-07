
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

import java.time.Instant;

public class Salt {

    //TODO hard coded for the moment
    private static final short HARD_CODED_SALT = 0;

    //TODO either the salt's short values need to match the order
    //of the time buckets they represent or we need a comparator to
    //do that for us. This is because we need to be able to group the returned
    //rows by salt and then quickly put each group into order without having to order
    //by the underlying event times

    //static methods only
    private Salt() {
    }

    public static short computeSalt(Instant time, int saltCount){
        //TODO hard coded for the moment
        return  HARD_CODED_SALT;
    }

    public static short[] getAllSalts() {
        //TODO hard coded for the moment
        return new short[]{HARD_CODED_SALT};
    }

}
