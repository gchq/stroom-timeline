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
package stroom.timeline.model;

/**
 * Used to provide an identifier that is globally unique and also
 * defines an order of events in cases where the event time is not
 * sufficiently granular to do this.
 */
public interface SequentialIdentifierProvider {

    /**
     * @return The sequence number as a byte array. The identifier needs
     * to be converted into bytes in such a way that if idB > idB then
     * idB.getBytes() > idA.getBytes() lexicographically.
     *
     * Also the byte[] representation of the identifier should ideally
     * be as compact as possible.
     */
    byte[] getBytes();

    /**
     * @return A human readbale representation of the identifier
     */
    String toHumanReadable();
}
