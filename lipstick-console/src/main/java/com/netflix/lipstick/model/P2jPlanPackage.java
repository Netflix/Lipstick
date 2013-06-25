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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

/**
 * Top level Lipstick model object.
 *
 * @author jmagnusson
 *
 */
@Entity
public class P2jPlanPackage {

    private long id;
    private P2jPlan optimized;
    private P2jScripts scripts;
    private P2jPlanStatus status;
    private P2jPlan unoptimized;
    private String userName;
    private String jobName;
    private String uuid;
    private Map<String, P2jSampleOutputList> sampleOutputMap;

    /**
     * Construct an empty P2jPlanPackage.
     */
    public P2jPlanPackage() {
    }

    /**
     * Construct a new P2jPlanPackage.
     *
     * @param optimized the optimized P2jPlan
     * @param unoptimized the unoptimized P2jPlan
     * @param script the script the plans were derived from
     * @param uuid a unique identifier for this object
     */
    public P2jPlanPackage(P2jPlan optimized, P2jPlan unoptimized, String script, String uuid) {
        this.optimized = optimized;
        this.unoptimized = unoptimized;
        this.scripts = new P2jScripts(script);
        this.status = new P2jPlanStatus();
        this.uuid = uuid;
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    @OneToOne(cascade = CascadeType.ALL)
    public P2jPlan getOptimized() {
        return optimized;
    }

    @OneToOne(cascade = CascadeType.ALL)
    public P2jScripts getScripts() {
        return scripts;
    }

    @OneToOne(cascade = CascadeType.ALL)
    public P2jPlanStatus getStatus() {
        return status;
    }

    @OneToOne(cascade = CascadeType.ALL)
    public P2jPlan getUnoptimized() {
        return unoptimized;
    }

    public String getUserName() {
        return userName;
    }

    public String getJobName() {
        return jobName;
    }

    @Column(unique = true)
    public String getUuid() {
        return uuid;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setOptimized(P2jPlan optimized) {
        this.optimized = optimized;
    }

    public void setScripts(P2jScripts scripts) {
        this.scripts = scripts;
    }

    public void setStatus(P2jPlanStatus status) {
        this.status = status;
    }

    public void setUnoptimized(P2jPlan unoptimized) {
        this.unoptimized = unoptimized;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @OneToMany(cascade = CascadeType.ALL)
    public Map<String, P2jSampleOutputList> getSampleOutputMap() {
        return sampleOutputMap;
    }

    public void setSampleOutputMap(Map<String, P2jSampleOutputList> sampleOutputMap) {
        this.sampleOutputMap = sampleOutputMap;
    }

}
