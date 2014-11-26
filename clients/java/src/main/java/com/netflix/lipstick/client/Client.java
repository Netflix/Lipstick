package com.netflix.lipstick.client;

import com.netflix.lipstick.graph.Graph;
import com.netflix.lipstick.template.Template;

public class Client extends BaseClient {
    
    protected static String JOB_PATH = "/v1/job";
    protected static String TEMPLATE_PATH = "/template";
    
    public Client(String serviceUrl) {
        super(serviceUrl);
    }
    
    public Client(String serviceUrl, int connectTimeout, int readTimeout) {
        super(serviceUrl, connectTimeout, readTimeout);
    }
    
    public Graph get(String graphId) {
        String path = String.format("%s/%s", JOB_PATH, graphId);
        String response = makeRequest(path, null, RequestVerb.GET);
        if (response != null) {
            return Graph.fromJson(response);
        }
        return null;
    }
    
    public String list() {
        return makeRequest(JOB_PATH, null, RequestVerb.GET);
    }
    
    public String save(Graph graph) {
        return makeRequest(JOB_PATH, graph, RequestVerb.POST);
    }
    
    public String update(Graph graph) {
        String path = String.format("%s/%s", JOB_PATH, graph.id);
        return makeRequest(path, graph, RequestVerb.PUT);
    }
    
    public String getTemplate(String name) {
        String path = String.format("%s/%s", TEMPLATE_PATH, name);
        return makeRequest(path, null, RequestVerb.GET);
    }
    
    public String saveTemplate(Template template) {
        String path = String.format("%s/%s", TEMPLATE_PATH, template.name);
        return makeRequest(path, template, RequestVerb.POST); 
    }
    
    public String listTemplates() {
        return makeRequest(TEMPLATE_PATH, null, RequestVerb.GET);
    }
}
