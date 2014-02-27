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
 * Container for a warning, a map containing attributes specific to
 * a single warning about a pig script or map/reduce job.
 * @author mroddy
 *
 */
@Entity
public class P2jWarning {
    private Map<String, String> warningAttributes = Maps.newHashMap();
    private long id;
    private String jobId;
    private String warningKey;

    @ElementCollection
    public Map<String, String> getWarningAttributes() {
        return warningAttributes;
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    public void setWarningAttributes(Map<String, String> warningAttributes) {
        this.warningAttributes = warningAttributes;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getWarningKey() {
        return warningKey;
    }

    public void setWarningKey(String warningKey) {
        this.warningKey = warningKey;
    }

}
