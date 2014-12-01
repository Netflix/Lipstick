package com.netflix.lipstick.graph;

import java.util.Map;

import jersey.repackaged.com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(value=JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Edge {
    public String u;
    public String v;
    public String type;
    public String label;
    public Map<String, Object> properties;
    
    public Edge() {
        this.properties = Maps.newHashMap();
    }
    
    public Edge(String u, String v) {
        this.u = u;
        this.v = v;
        this.properties = Maps.newHashMap();
    }
    
    public Edge u(String u) {
        this.u = u;
        return this;        
    }
    
    public Edge v(String v) {
        this.v = v;
        return this;
    }
    
    public Edge type(String type) {
        this.type = type;
        return this;
    }
    
    public Edge label(String label) {
        this.label = label;
        return this;
    }
    
    public Object property(String key) {
        return this.properties.get(key);
    }
    
    public Edge property(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }
    
    public Edge properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }
    
    public boolean equals(Object other) {        
        if (this == other) return true;
        if (!(other instanceof Edge)) return false;
        
        Edge e = (Edge)other;
        
        return
                this.u == null ? e.u == null : this.u.equals(e.u) &&
                this.v == null ? e.v == null : this.v.equals(e.v) &&
                this.type == null ? e.type == null : this.type.equals(e.type) &&
                this.label == null ? e.label == null : this.label.equals(e.label) &&
                this.properties.equals(e.properties);
    }
}
