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
package com.netflix.lipstick.model;

import java.util.Map;

import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import com.google.common.collect.Maps;
/**
 * Container for Map/Reduce job counters.
 * @author jmagnusson
 *
 */
@Entity
public class P2jCounters {
    private Map<String, Long> counters = Maps.newHashMap();
    private long id;

    @ElementCollection
    public Map<String, Long> getCounters() {
        return counters;
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    public void setCounters(Map<String, Long> counters) {
        this.counters = counters;
    }

    public void setId(long id) {
        this.id = id;
    }

}
