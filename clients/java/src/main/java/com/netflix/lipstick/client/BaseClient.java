package com.netflix.lipstick.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.ObjectMapper;


public class BaseClient {
    protected enum RequestVerb {
        POST, PUT, GET
    };
    
    private static final Log LOG = LogFactory.getLog(BaseClient.class);
    private static int DEFAULT_TIMEOUT = 1500;
    
    protected String serviceUrl;
    protected ClientConfig config;    
    protected ObjectMapper om = new ObjectMapper();
    
    public static class Server implements Comparable<Server> {
        public String url;
        public Long penalty;

        public Server(String url, Long penalty) {
            this.url = url;
            this.penalty = penalty;
        }

        public void penalize() {
            this.penalty = penalty*2l; // Double penalty each time
        }

        public int compareTo(Server other) {
            return penalty.compareTo(other.penalty);
        }
    }
    protected PriorityQueue<Server> servers = null;
    
    public BaseClient(String serviceUrls) {
        this(serviceUrls, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT);        
    }
    
    public BaseClient(String serviceUrls, int connectTimeout, int readTimeout) {
        LOG.info("Initializing " + this.getClass() + " with serviceUrls: " + serviceUrls);        
        initializeServers(serviceUrls);
        config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        config.property(ClientProperties.READ_TIMEOUT, readTimeout);        
    }
    
    protected void initializeServers(String serviceUrls) {
        String[] urls = serviceUrls.split(",");
        servers = new PriorityQueue<Server>(urls.length);        
        for (String url : urls) {
            servers.add(new Server(url, 1l));
        }
    }
    
    protected void rebuildServers(List<Server> servers) {
        for (Server s : servers) {
            this.servers.add(s);
        }        
    }
    
    protected String getServiceUrl() {
        Server s = servers.peek();
        return s.url;
    }
    
    protected String makeRequest(String resource, Object requestObj, RequestVerb verb) {
        List<Server> penalized = new ArrayList<Server>();
        
        javax.ws.rs.client.Client c = ClientBuilder.newClient(config);
        Response response = null;
        
        while (servers.size() > 0) {
            String serviceUrl = getServiceUrl();
            LOG.info("Trying Lipstick server "+serviceUrl);
            WebTarget target = c.target(serviceUrl).path(resource);            
            response = sendRequest(target, requestObj, verb);
            if (response != null) {
                rebuildServers(penalized);
                return response.readEntity(String.class);
            } else {
                Server s = servers.poll();
                s.penalize();
                penalized.add(s);
            }                
        }
        rebuildServers(penalized);
        return null;
    }
    
    protected Response sendRequest(WebTarget target, Object requestObj, RequestVerb verb) {
        Response response = null;
        try {
            switch(verb) {
            case POST:
                response = target.request().post(Entity.entity(om.writeValueAsString(requestObj), MediaType.APPLICATION_JSON_TYPE));
                break;
            case PUT:
                response = target.request().put(Entity.entity(om.writeValueAsString(requestObj), MediaType.APPLICATION_JSON_TYPE));
                break;
            case GET:
                response = target.request().get();
                break;
            }
            response.bufferEntity();
            if (response.getStatus() == 200) {
                return response;
            } else {
                handleStatus(response.getStatus(), response.readEntity(String.class));
            }
        } catch (Exception e) {
            if (response != null) {
                LOG.error(String.format("Error contacting Lipstick server. code: [%d], message: [%s]", response.getStatus(), response.readEntity(String.class)));
            } else {
                LOG.error(String.format("Error contacting Lipstick server."));
            }
            LOG.debug("Stacktrace", e);
        }
        return null;
    }
    
    protected void handleStatus(int status, String message) throws IOException {
        switch(status) {
        case 404:
            throw new NoSuchResourceException(String.format("%d not found, message: [%s]", status, message));
        case 400:
            throw new MalformedRequestException(String.format("%d bad request, message: [%s]", status, message));
        case 500:
            throw new RemoteServerException(String.format("%d internal server error, message: [%s]", status, message));
        default:
            LOG.debug(String.format("failed: %s message: %s", status, message));
        }
    }
    
    public static class NoSuchResourceException extends IOException {
        public NoSuchResourceException(String message) {
            super(message);
        }
    }

    public static class MalformedRequestException extends IOException {
        public MalformedRequestException(String message) {
            super(message);
        }
    }

    public static class RemoteServerException extends IOException {
        public RemoteServerException(String message) {
            super(message);
        }
    }
}
