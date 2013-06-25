/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.lipstick.util;

import java.util.Iterator;

/**
 * Utility to facilitate easy iteration.
 * @author nbates
 *
 * @param <T>
 */
public class EzIterable<T> implements Iterable<T> {
    private final Iterator<T> iter;

    /**
     * Constructs an EzIterable with the given iterator.
     *
     * @param iter
     */
    public EzIterable(Iterator<T> iter) {
        this.iter = iter;
    }

    @Override
    public Iterator<T> iterator() {
        return iter;
    }

    /**
     * Returns a strongly typed EzIterable based on the input iterator.
     *
     * @param iter
     * @return
     */
    public static <T> EzIterable<T> getIterable(Iterator<T> iter) {
        return new EzIterable<T>(iter);
    }
}
