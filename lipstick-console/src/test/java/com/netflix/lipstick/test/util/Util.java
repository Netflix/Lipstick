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
package com.netflix.lipstick.test.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class Util {
    public static <T> HashSet<T> safeNewSet(Collection<T> coll) {
        if (coll == null) {
            return new HashSet<T>();
        }

        return new HashSet<T>(coll);
    }

    public static <T> SetView<T> safeDiffSets(Set<T> left, Set<T> right) {
        if (left == null) {
            left = new HashSet<T>();
        }
        if (right == null) {
            right = new HashSet<T>();
        }

        return Sets.difference(left, right);
    }
}
