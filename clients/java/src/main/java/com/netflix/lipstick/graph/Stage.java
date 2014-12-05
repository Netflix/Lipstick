package com.netflix.lipstick.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(value=JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Stage {
    public String name;
    public Status status;
    
    public Stage() {        
    }
    
    public Stage(String name) {
        this(name, new Status());
    }
    
    public Stage(String name, Status status) {
        this.name = name;
        this.status = status;
    }
    
    public Stage name(String name) {
        this.name = name;
        return this;
    }
    
    public Stage status(Status status) {
        this.status = status;
        return this;
    }
    
    public boolean equals(Object other) {        
        if (this == other) return true;
        if (!(other instanceof Stage)) return false;
        
        Stage s = (Stage)other;
        
        return
                this.name == null ? s.name == null : this.name.equals(s.name) &&
                this.status == null ? s.status == null : this.status.equals(s.status);
    }
}
