package com.netflix.lipstick.graph;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Maps;

@JsonInclude(value=JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class NodeGroup {
    public String id;
    public String url;
    public List<String> children;
    public Map<String, Object> properties;
    
    public NodeGroup() {
        this.properties = Maps.newHashMap();
    }
    
    public NodeGroup(String id) {
        this.id = id;
        this.properties = Maps.newHashMap();
    }
    
    public NodeGroup id(String id) {
        this.id = id;
        return this;
    }
    
    public NodeGroup url(String url) {
        this.url = url;
        return this;        
    }
    
    public NodeGroup children(List<String> children) {
        this.children = children;
        return this;
    }
    
    public NodeGroup properties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }
}
