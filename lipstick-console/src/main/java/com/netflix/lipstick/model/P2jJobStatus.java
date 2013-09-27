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
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * Container for the status of an individual map/reduce job.
 * @author jmagnusson
 *
 */
@Entity
public class P2jJobStatus {

    private Map<String, P2jCounters> counters;
    private String scope;
    private String jobId;
    private String jobName;
    private String trackingUrl;
    private boolean isComplete;
    private boolean isSuccessful;
    private float mapProgress;
    private float reduceProgress;
    private int totalMappers;
    private int totalReducers;
    private long id;
    private long startTime;
    private long finishTime;

    /**
     * Initialize an empty P2jJobStatus object.
     */
    public P2jJobStatus() {
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @OneToMany(cascade = CascadeType.ALL)
    public Map<String, P2jCounters> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, P2jCounters> counters) {
        this.counters = counters;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public boolean getIsComplete() {
        return isComplete;
    }

    public void setIsComplete(Boolean isComplete) {
        this.isComplete = isComplete;
    }

    public boolean getIsSuccessful() {
        return isSuccessful;
    }

    public void setIsSuccessful(boolean isSuccessful) {
        this.isSuccessful = isSuccessful;
    }

    public float getMapProgress() {
        return mapProgress;
    }

    public void setMapProgress(float mapProgress) {
        this.mapProgress = mapProgress;
    }

    public float getReduceProgress() {
        return reduceProgress;
    }

    public void setReduceProgress(float reduceProgress) {
        this.reduceProgress = reduceProgress;
    }

    public int getTotalMappers() {
        return totalMappers;
    }

    public void setTotalMappers(int totalMappers) {
        this.totalMappers = totalMappers;
    }

    public int getTotalReducers() {
        return totalReducers;
    }

    public void setTotalReducers(int totalReducers) {
        this.totalReducers = totalReducers;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

}
