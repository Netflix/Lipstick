package com.netflix.lipstick.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(value=JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Status {
    public Integer progress;
    public Long startTime;
    public Long endTime;
    public Long heartbeatTime;
    public String statusText;

    public Status() {
        Long now = System.currentTimeMillis();
        progress = 0;
        startTime = now;
        heartbeatTime = now;        
    }
    
    public Status(Integer progress, Long startTime, Long heartbeatTime, String statusText) {
        this.progress = progress;
        this.startTime = startTime;
        this.heartbeatTime = heartbeatTime;
        this.statusText = statusText;
    }
    
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
    
    public boolean equals(Object other) {        
       if (this == other) return true;
       if (!(other instanceof Status)) return false;
        
       Status s = (Status)other;
        
        return
                this.progress.equals(s.progress) &&
                this.startTime.equals(s.startTime) &&
                this.endTime == null ? s.endTime == null : this.endTime.equals(s.endTime) &&
                this.heartbeatTime.equals(s.heartbeatTime) &&
                this.statusText.equals(s.statusText);
    }
}
