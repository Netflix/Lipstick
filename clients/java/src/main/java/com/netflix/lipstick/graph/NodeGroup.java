package com.netflix.lipstick.graph;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@JsonInclude(value=JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class NodeGroup {
    public String id;
    public String url;
    public Status status;
    public List<String> children;
    public Map<String, Object> properties;
    
    public NodeGroup() {
        this.properties = Maps.newHashMap();
        this.children = Lists.newArrayList();
    }
    
    public NodeGroup(String id) {
        this.id = id;
        this.properties = Maps.newHashMap();
        this.children = Lists.newArrayList();
    }
    
    public NodeGroup id(String id) {
        this.id = id;
        return this;
    }
    
    public NodeGroup url(String url) {
        this.url = url;
        return this;        
    }
    
    public NodeGroup status(Status status) {
        this.status = status;
        return this;
    }
    
    public Boolean hasChild(String child) {
        return this.children.contains(child);
    }
    
    public NodeGroup child(String child) {
        if (!hasChild(child)) {
            this.children.add(child);
        }
        return this;
    }
    
    public NodeGroup children(List<String> children) {
        this.children = children;
        return this;
    }
    
    public Object property(String key) {
        return this.properties.get(key);
    }
    
    public NodeGroup property(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }
    
    public NodeGroup properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }
}
