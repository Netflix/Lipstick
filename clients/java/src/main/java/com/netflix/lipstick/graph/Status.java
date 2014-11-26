package com.netflix.lipstick.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(value=JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Status {
    public Integer progress;
    public Long startTime;
    public Long endTime;
    public Long heartbeatTime;
    public String statusText;
    
    public Status progress(Integer progress) {
        this.progress = progress;
        return this;
    }
    
    public Status startTime(Long startTime) {
        this.startTime = startTime;
        return this;
    }
    
    public Status endTime(Long endTime) {
        this.endTime = endTime;
        return this;
    }
    
    public Status heartbeatTime(Long heartbeatTime) {
        this.heartbeatTime = heartbeatTime;
        return this;
    }
    
    public Status statusText(String statusText) {
        this.statusText = statusText;
        return this;
    }
}
