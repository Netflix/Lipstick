package com.netflix.lipstick.graph;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Maps;

@JsonInclude(value=JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Node {
    public String id;
    public String url;
    public String type;
    public String child;
    public Status status;
    public Map<String, Object> properties;
    
    public Node() {
        this.properties = Maps.newHashMap();
    }
    
    public Node(String id) {
        this.id = id;
        this.properties = Maps.newHashMap();        
    }
    
    public Node id(String id) {
        this.id = id;
        return this;        
    }
    
    public Node url(String url) {
        this.url = url;
        return this;
    }
    
    public Node type(String type) {
        this.type = type;
        return this;
    }
    
    public Node child(String child) {
        this.child = child;
        return this;
    }
    
    public Node status(Status status) {
        this.status = status;
        return this;
    }
    
    public Object property(String key) {
        return this.properties.get(key);
    }
    
    public Node property(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }
    
    public Node properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }
    
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Node)) return false;
        
        Node n = (Node)other;
        
        return
                this.id == null ? n.id == null : this.id.equals(n.id) &&
                this.url == null ? n.url == null : this.url.equals(n.url) &&
                this.type == null ? n.type == null : this.type.equals(n.type) &&
                this.child == null ? n.child == null : this.child.equals(n.child) &&
                this.status == null ? n.status == null : this.status.equals(n.status) &&
                this.properties.equals(n.properties);
    }
}
