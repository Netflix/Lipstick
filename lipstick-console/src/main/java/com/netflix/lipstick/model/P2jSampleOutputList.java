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

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

import com.google.common.collect.Lists;

/**
 * Container for the set of sample output data produced by a map/reduce job.
 *
 * @author jmagnusson
 *
 */
@Entity
public class P2jSampleOutputList {

    private List<P2jSampleOutput> sampleOutputList;
    private long id;

    /**
     * Construct an empty P2jSampleOutputList.
     */
    public P2jSampleOutputList() {
    }

    @OneToMany(cascade = CascadeType.ALL)
    public List<P2jSampleOutput> getSampleOutputList() {
        return sampleOutputList;
    }

    public void setSampleOutputList(List<P2jSampleOutput> sampleOutputList) {
        this.sampleOutputList = sampleOutputList;
    }

    /**
     * Add a new P2jSampleOutput to the P2jSampleOutputList.
     *
     * @param sampleOutput the P2jSampleOutput to add
     */
    public void add(P2jSampleOutput sampleOutput) {
        if (sampleOutputList == null) {
            sampleOutputList = Lists.newArrayList();
        }
        sampleOutputList.add(sampleOutput);
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

}
