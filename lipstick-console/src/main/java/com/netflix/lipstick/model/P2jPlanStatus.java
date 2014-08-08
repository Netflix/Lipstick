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

import java.util.Date;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Transient;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.google.common.collect.Maps;


/**
 * Lipstick Pig job overall status.
 *
 * @author jmagnusson
 *
 */
@Entity
public class P2jPlanStatus {

    private long id;
    private Map<String, P2jJobStatus> jobStatusMap;

    private int progress = 0;

    private Date startTime = null;
    private Date endTime = null;
    private Date heartbeatTime = null;
    private StatusText statusText = null;

    public static enum StatusText {
        finished, running, terminated, failed
    }

    /**
     * Creates a default P2jPlanStatus object.
     */
    public P2jPlanStatus() {
        jobStatusMap = Maps.newHashMap();
    }

    @Id
    @GeneratedValue
    public long getId() {
        return id;
    }

    /**
     * Returns a P2jJobStatus based on the given jid.
     * If there is no job with the given jid, return null.
     *
     * @param jid
     * @return
     */
    public P2jJobStatus getJob(String jid) {
        if (hasJob(jid)) {
            return jobStatusMap.get(jid);
        }
        return null;
    }

    @Transient
    @JsonIgnore
    public Set<String> getJobList() {
        return jobStatusMap.keySet();
    }

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    public Map<String, P2jJobStatus> getJobStatusMap() {
        return jobStatusMap;
    }

    /**
     * Returns the current progress for the P2jPlanStatus object.
     * Progress should have the range [0, 100].
     *
     * @return
     */
    public int getProgress() {
        return progress;
    }

    /**
     * Returns the current end time for the P2jPlanStatus object.
     * End time should be null if the plan hasn't finished.
     *
     * @return
     */
    public Date getEndTime() {
        return endTime;
    }

    /**
     * Returns the current start time for the P2jPlanStatus object.
     * Start time should be null if the plan hasn't started.
     *
     * @return
     */
    public Date getStartTime() {
        return startTime;
    }

    @Enumerated(EnumType.STRING)
    public StatusText getStatusText() {
        return statusText;
    }

    /**
     * Returns the most recent heartbeat time for the P2jPlanStatus object.
     *
     * @return
     */
    public Date getHeartbeatTime() {
        return heartbeatTime;
    }

    /**
     * Sets the statusText for the P2jPlanStatus object.
     *
     * @param statusText
     */
    public void setStatusText(StatusText statusText) {
        this.statusText = statusText;
    }

    /**
     * Checks if the plan has a job with the
     * id of the passed in P2jJobStatus.
     *
     * @param job
     * @return
     */
    public boolean hasJob(P2jJobStatus job) {
        return hasJob(job.getJobId());
    }

    /**
     * Checks if the plan has a job with
     * id matching the given jid.
     *
     * @param jid
     * @return
     */
    public boolean hasJob(String jid) {
        return jobStatusMap.containsKey(jid);
    }

    /**
     * Sets the id for the P2jPlanStatus object.
     *
     * @param id
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Sets the jobStatusMap for the P2jPlanStatus object.
     *
     * @param jobStatusMap
     */
    public void setJobStatusMap(Map<String, P2jJobStatus> jobStatusMap) {
        this.jobStatusMap = jobStatusMap;
    }

    /**
     * Sets the progress for the P2jPlanStatus object.
     *
     * @param progress
     * @return
     */
    public P2jPlanStatus setProgress(int progress) {
        this.progress = progress;
        return this;
    }

    /**
     * Sets the startTime for the P2jPlanStatus object to the current time.
     *
     * @return
     */
    public P2jPlanStatus setStartTime() {
        startTime = new Date();
        return this;
    }

    /**
     * Sets the startTime for the P2jPlanStatus object.
     *
     * @param startTime
     * @return
     */
    public P2jPlanStatus setStartTime(Date startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * Sets the endTime for the P2jPlanStatus object to the current time.
     *
     * @return
     */
    public P2jPlanStatus setEndTime() {
        endTime = new Date();
        return this;
    }

    /**
     * Sets the endTime for the P2jPlanStatus object.
     *
     * @param endTime
     * @return
     */
    public P2jPlanStatus setEndTime(Date endTime) {
        this.endTime = endTime;
        return this;
    }

    /**
     * Sets the heartbeat time for the P2jPlanStatus object to the current time.
     */
    public void setHeartbeatTime() {
        this.heartbeatTime = new Date();
    }

    /**
     * Sets the heartbeat time for the P2jPlanStatus object.
     *
     * @param heartbeatTime
     */
    public void setHeartbeatTime(Date heartbeatTime) {
        this.heartbeatTime = heartbeatTime;
    }

    /**
     * Updates the job reference that the plan has
     * based on id of the passed in P2jJobStatus.
     *
     * @param job
     * @return
     */
    public P2jPlanStatus updateWith(P2jJobStatus job) {
        jobStatusMap.put(job.getJobId(), job);
        return this;
    }

    /**
     * Updates this plan with non-null information from the passed in plan.
     *
     * @param plan
     * @return
     */
    public P2jPlanStatus updateWith(P2jPlanStatus plan) {
        if (plan.getProgress() > 0) {
            setProgress(plan.getProgress());
        }
        if (plan.getStartTime() != null) {
            setStartTime(plan.getStartTime());
        }
        if (plan.getEndTime() != null) {
            setEndTime(plan.getEndTime());
        }
        if (plan.getHeartbeatTime() != null) {
            setHeartbeatTime(plan.getHeartbeatTime());
        }
        if (plan.getStatusText() != null) {
            setStatusText(plan.getStatusText());
        }
        for (String jid : plan.getJobList()) {
            this.updateWith(plan.getJob(jid));
        }
        return this;
    }

}
