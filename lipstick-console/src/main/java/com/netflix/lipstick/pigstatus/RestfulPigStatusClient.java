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
package com.netflix.lipstick.pigstatus;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlanStatus;
import com.netflix.lipstick.model.P2jSampleOutputList;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * RESTful client implementation of PigStatusClient.
 *
 * @author nbates
 *
 */
public class RestfulPigStatusClient implements PigStatusClient {
    protected enum RequestVerb {
        POST, PUT
    };

    private static final Log LOG = LogFactory.getLog(RestfulPigStatusClient.class);

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
            
    protected PriorityQueue<Server> lipstickServers = null;

    /**
     * Constructs a default RestfulPigStatusClient.
     */
    public RestfulPigStatusClient() {
    }
    
    /**
     * Constructs a RestfulPigStatusClient with the given serviceUrls.
     *
     * @param serviceUrls
     */
    public RestfulPigStatusClient(String serviceUrls) {
        LOG.info("Initializing " + this.getClass() + " with serviceUrls: " + serviceUrls);
        initializeServers(serviceUrls);
    }
    
    protected void initializeServers(String serviceUrls) {
        String[] urls = serviceUrls.split(",");
        lipstickServers = new PriorityQueue(urls.length);        
        for (String url : urls) {
            lipstickServers.add(new Server(url, 1l));
        }
    }

    protected void rebuildServers(List<Server> servers) {
        for (Server s : servers) {
            lipstickServers.add(s);
        }        
    }
    
    protected String getServiceUrl() {
        Server s = lipstickServers.peek();
        return s.url;
    }
    
    @Override
    public String savePlan(P2jPlanPackage plans) {
        plans.getStatus().setHeartbeatTime();
        ClientResponse response = makeRequest("/job/", plans, RequestVerb.POST);
        if (response == null) {
            return null;
        }

        try {
            String output = (String) om.readValue(response.getEntity(String.class), Map.class).get("uuid");
            if (!plans.getUuid().equals(output)) {
                LOG.error("Incorrect uuid returned from server");
            }
            String serviceUrl = getServiceUrl();
            LOG.info("This script has been assigned uuid: " + output);
            LOG.info("Navigate to " + serviceUrl + "#job/" + output + " to view progress.");
            return plans.getUuid();

        } catch (Exception e) {
            LOG.error("Error getting uuid from server response.", e);
        }
        return null;
    }

    @Override
    public void saveStatus(String uuid, P2jPlanStatus status) {
        status.setHeartbeatTime();
        String resource = "/job/" + uuid;
        makeRequest(resource, status, RequestVerb.PUT);
        
        String serviceUrl = getServiceUrl();
        LOG.info("Navigate to " + serviceUrl + "#job/" + uuid + " to view progress.");
    }

    @Override
    public void saveSampleOutput(String uuid, String jobId, P2jSampleOutputList sampleOutputList) {
        String resource = String.format("/job/%s/sampleOutput/%s", uuid, jobId);
        makeRequest(resource, sampleOutputList, RequestVerb.PUT);
    }    
    
    protected ClientResponse makeRequest(String resource, Object requestObj, RequestVerb verb) {
        List<Server> penalized = new ArrayList<Server>();
        
        Client client = Client.create();
        ClientResponse response = null;

        // Go through queue and get servers in increasing order of penalty
        while (lipstickServers.size() > 0) {
            String serviceUrl = getServiceUrl();
            LOG.info("Trying Lipstick server "+serviceUrl);
            String resourceUrl = serviceUrl + resource;        
            WebResource webResource = client.resource(resourceUrl);                           
            response = sendRequest(webResource, requestObj, verb);
            if (response != null) {
                rebuildServers(penalized);
                return response;
            } else {
                Server s = lipstickServers.poll();
                s.penalize();
                penalized.add(s);
            }                
        }
        rebuildServers(penalized);
        return null;
    }
    
    protected ClientResponse sendRequest(WebResource webResource, Object requestObj, RequestVerb verb) {

        ClientResponse response = null;
        try {

            String resourceUrl = webResource.getURI().toURL().toString();
            
            LOG.debug("Sending " + verb + " request to " + resourceUrl);
            LOG.debug(om.writeValueAsString(requestObj));

            switch (verb) {
            case POST:
                response = webResource.type("application/json").post(ClientResponse.class,
                                                                     om.writeValueAsString(requestObj));
                break;
            case PUT:
                response = webResource.type("application/json").put(ClientResponse.class,
                                                                    om.writeValueAsString(requestObj));
                break;
            default:                
                throw new RuntimeException("Invalid verb: " + verb + " for resourceUrl: " + resourceUrl);
            }

            if (response != null && response.getStatus() != 200) {
                LOG.error("Error contacting Lipstick server.  Received status code " + response.getStatus());
                LOG.debug(response.getEntity(String.class));
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }

            return response;

        } catch (Exception e) {
            LOG.error("Error contacting Lipstick server.");
            LOG.debug("Stacktrace", e);
        }

        return null;
    }
}
