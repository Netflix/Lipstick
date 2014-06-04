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
package com.netflix.lipstick;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.OriginalLocation;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.netflix.lipstick.adaptors.POJsonAdaptor;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLOLoad;
import com.netflix.lipstick.model.operators.P2jLOStore;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

public class TezPlanCalculator {
    private static final Log LOG = LogFactory.getLog(TezPlanCalculator.class);

    protected TezOperPlan tp;
    protected P2jPlan p2jPlan;
    protected Map<PhysicalOperator, Operator> phy2LogMap;
    protected Map<String, P2jLogicalRelationalOperator> p2jMap;
    protected Map<Operator, String> reverseMap;
    protected Map<String, Operator> locationMap;
    
    // Holds, for each TezOperator (Vertex), the number of logical operators
    protected Map<OperatorKey, Integer> operatorCounts;
    
    public TezPlanCalculator(P2jPlan p2jPlan,
                            TezOperPlan tp,
                            Map<PhysicalOperator, Operator> phy2LogMap,
                            Map<Operator, String> reverseMap) {
        this.tp = tp;
        this.phy2LogMap = phy2LogMap;
        this.p2jMap = p2jPlan.getPlan();
        this.p2jPlan = p2jPlan;
        this.reverseMap = reverseMap;
        this.locationMap = generateLocationMap();
        this.operatorCounts = Maps.newHashMap();
        p2jPlan.setPlan(assignVerticesToNodes());
    }

    public P2jPlan getP2jPlan() {
        return p2jPlan;
    }
    
    /**
     * Generate a map of pig script code location to logical operators.
     *
     * @return map of location to logical operator
     */
    protected Map<String, Operator> generateLocationMap() {
        Map<String, Operator> locationMap = Maps.newHashMap();
        for (Operator op : reverseMap.keySet()) {
            LogicalRelationalOperator logicalOp = (LogicalRelationalOperator) op;
            OriginalLocation loc = new OriginalLocation(logicalOp.getAlias(),
                                                        logicalOp.getLocation().line(),
                                                        logicalOp.getLocation().offset());
            locationMap.put(loc.toString(), logicalOp);
        }
        LOG.debug(locationMap);
        return locationMap;
    }

    protected Map<String, P2jLogicalRelationalOperator> assignVerticesToNodes() {
        for (TezOperator job : tp) {
            operatorCounts.put(job.getOperatorKey(), 0);
            assignVerticesToPlan(job.plan, job.getOperatorKey());
        }

        assignVerticesToUnknownNodes();
        
        for (TezOperator job : tp) {
            Integer count = operatorCounts.get(job.getOperatorKey());
            if (count < 1) {
                appendUnknownOperators(job.getOperatorKey());
            }
        }
        
        connectUnknownOperators();
        p2jPlan.setPlan(p2jMap);
        return p2jMap;
    }

    protected void assignVerticesToPlan(PhysicalPlan pp, OperatorKey job) {
        for (PhysicalOperator pop : pp) {
            assignVertex(pop, job);
        }
    }

    protected P2jLogicalRelationalOperator getOpForStore(POStore pop) {
        FileSpec pofs = pop.getSFile();
        for (Entry<String, P2jLogicalRelationalOperator> entry : p2jMap.entrySet()) {
            if (entry.getValue() instanceof P2jLOStore) {
                P2jLOStore store = (P2jLOStore) entry.getValue();
                if (store.getStorageLocation().equals(pofs.getFileName())
                    && pofs.getFuncName().endsWith(store.getStorageFunction())) {
                    return store;
                }
            }
        }
        return null;
    }
     
    protected P2jLogicalRelationalOperator getOpForLoad(POLoad pop) {
        FileSpec pofs = pop.getLFile();
        for (Entry<String, P2jLogicalRelationalOperator> entry : p2jMap.entrySet()) {
            if (entry.getValue() instanceof P2jLOLoad) {
                P2jLOLoad load = (P2jLOLoad) entry.getValue();                
                if (load.getAlias().equals(pop.getAlias()) && load.getStorageLocation().equals(pofs.getFileName())
                    && pofs.getFuncName().endsWith(load.getStorageFunction())) {
                    return load;
                }
            }
        }
        return null;
    }
    
    protected void assignVertex(PhysicalOperator pop, OperatorKey job) {

    	String jid = job.toString();
    	
        // special cases - find other operators inside these that need to be
        // assigned
        if (pop instanceof POLocalRearrange) {
            for (PhysicalPlan ipl : ((POLocalRearrange) pop).getPlans()) {
                assignVerticesToPlan(ipl, job);
            }
        } else if (pop instanceof PODemux) {
            for (PhysicalPlan ipl : ((PODemux) pop).getPlans()) {
                assignVerticesToPlan(ipl, job);
            }
        } else if (pop instanceof POPreCombinerLocalRearrange) {
            for (PhysicalPlan ipl : ((POPreCombinerLocalRearrange) pop).getPlans()) {
                assignVerticesToPlan(ipl, job);
            }       
        } else if (pop instanceof POSplit) {
            for (PhysicalPlan ipl : ((POSplit) pop).getPlans()) {
                assignVerticesToPlan(ipl, job);
            }
        }

        String stepTypeString = "TezVertex";
        
        if (pop instanceof POStore) {
            P2jLogicalRelationalOperator node = getOpForStore((POStore) pop);
            if (node != null) {
            	operatorCounts.put(job, operatorCounts.get(job)+1);
                node.setMapReduce(jid, stepTypeString);
                return;
            }
        } else if (pop instanceof POLoad) {
            P2jLogicalRelationalOperator node = getOpForLoad((POLoad) pop);
            if (node != null) {
            	operatorCounts.put(job, operatorCounts.get(job)+1);
                node.setMapReduce(jid, stepTypeString);
                return;
            }
        } else if (phy2LogMap.containsKey(pop) && reverseMap.containsKey(phy2LogMap.get(pop))) {
            String nodeId = reverseMap.get(phy2LogMap.get(pop));
            P2jLogicalRelationalOperator node = p2jMap.get(nodeId);
            operatorCounts.put(job, operatorCounts.get(job)+1);
            node.setMapReduce(jid, stepTypeString);
            LOG.debug("Found key for: " + pop.toString());
            return;
        } else {
            LOG.debug("No node for pop: " + pop + pop.getClass() + " ... Searching locationMap.");
            boolean didAssign = false;
            for (OriginalLocation loc : pop.getOriginalLocations()) {
                LOG.debug("Checking location: " + loc);
                if (locationMap.containsKey(loc.toString())) {
                    P2jLogicalRelationalOperator node = p2jMap.get(reverseMap.get(locationMap.get(loc.toString())));
                    LOG.debug("Found location... " + node);
                    if (node.getMapReduce() == null) {
                    	operatorCounts.put(job, operatorCounts.get(job)+1);
                        node.setMapReduce(jid, stepTypeString);
                        didAssign = true;
                        LOG.debug("Assign location... " + node);
                    }
                }
            }
            if (didAssign) {
                return;
            }
        }
        LOG.debug("*** Couldn't assign " + pop.getClass() + pop);
    }

    protected void assignVerticesToUnknownNodes() {
        for (P2jLogicalRelationalOperator node : p2jMap.values()) {
            if (node.getMapReduce() == null || node.getMapReduce().getJobId() == null) {                
                String jobId = resolveJobForNode(node);
                if (jobId != null) {
                	incrementOperatorCount(jobId, 1);
                    node.setMapReduce(jobId, "TezVertex");
                }
            }
        }
    }

    protected String resolveJobForNode(P2jLogicalRelationalOperator node) {
        Set<String> pred = generateScopesForNode(node, new ScopeGetter() {
            @Override
            public List<String> getScopes(P2jLogicalRelationalOperator node) {
                return node.getPredecessors();
            }
        });
        Set<String> succ = generateScopesForNode(node, new ScopeGetter() {
            @Override
            public List<String> getScopes(P2jLogicalRelationalOperator node) {
                return node.getSuccessors();
            }
        });
        SetView<String> intersect = Sets.intersection(pred, succ);
        if (intersect.size() > 0) {
            return intersect.iterator().next();
        } else if (succ.size() == 1) {
            return succ.iterator().next();
        } else if (pred.size() > 0) {
            return pred.iterator().next();
        } else if (succ.size() > 0) {
            return succ.iterator().next();
        }
        return null;
    }
    
    /**
     * Increment the number of logical operators that map to a given TezOperator's
     * plan.
     * @param jobId The jobId of the TezOperator to update
     * @param update The integer amount to update the TezOperator's count
     */
    protected void incrementOperatorCount(String jobId, Integer update) {
    	for (OperatorKey key : operatorCounts.keySet()) {
    		if (key.toString().equals(jobId)) {
    			operatorCounts.put(key, operatorCounts.get(key) + update);
    			return;
    		}
    	}
    }
    
    interface ScopeGetter {
        List<String> getScopes(P2jLogicalRelationalOperator node);
    }

    /**
     * Generate the set of map reduce jobs accessible from node in the direction
     * defined by ScopeGetter.
     *
     * @param node
     *            the P2jLogicalRelationalOperator to search from
     * @param scopeGetter
     *            the ScopeGetter defining the direction of the search
     * @return a set of map/reduce job scopes
     */
    protected Set<String> generateScopesForNode(P2jLogicalRelationalOperator node, ScopeGetter scopeGetter) {
        Set<String> scopes = Sets.newHashSet();
        for (String id : scopeGetter.getScopes(node)) {
            P2jLogicalRelationalOperator job = p2jMap.get(id);
            if (job.getMapReduce() != null && job.getMapReduce().getJobId() != null) {
                scopes.add(job.getMapReduce().getJobId());
            } else {
                scopes.addAll(generateScopesForNode(job, scopeGetter));
            }
        }
        return scopes;
    }

    protected List<P2jLogicalRelationalOperator> p2jOpsForJobId(String jobId) {
    	List<P2jLogicalRelationalOperator> result = Lists.newArrayList();
    	for (P2jLogicalRelationalOperator op : p2jMap.values()) {
    		if (op.getMapReduce().getJobId().equals(jobId)) {
    			result.add(op);
    		}
    	}
    	return result;
    }
    
    protected List<P2jLogicalRelationalOperator> getOutBoundaryNodes(String jobId) {
    	List<P2jLogicalRelationalOperator> result = Lists.newArrayList();
    	for (P2jLogicalRelationalOperator p2jOp : p2jOpsForJobId(jobId)) {
    	    if (p2jOp.getSuccessors().size() == 0) {
    	        result.add(p2jOp);
    	    } else {
    	        for (String id : p2jOp.getSuccessors()) {
    	            P2jLogicalRelationalOperator succ = p2jMap.get(id);
    	            if (!succ.getMapReduce().getJobId().equals(jobId)) {
    	                result.add(p2jOp);
    	                break;
    	            }
    	        }
    	    }
    	}
    	return result;
    }
    
    protected List<P2jLogicalRelationalOperator> getInBoundaryNodes(String jobId) {
    	List<P2jLogicalRelationalOperator> result = Lists.newArrayList();
    	for (P2jLogicalRelationalOperator p2jOp : p2jOpsForJobId(jobId)) {
    	    if (p2jOp.getPredecessors().size() == 0) {
    	        result.add(p2jOp); // it's a boundary node even if it doesn't have other nodes
    	        // linking to it yet
    	    } else {
    	         for (String id : p2jOp.getPredecessors()) {
    	             P2jLogicalRelationalOperator pred = p2jMap.get(id);
    	             if (!pred.getMapReduce().getJobId().equals(jobId)) {
    	                 result.add(p2jOp);
    	                 break;
    	             }
    	         }
    	     }
    	}
    	return result;
    }
    
    protected List<String> getSourceNodeIds(String jobId) {
        List<String> result = Lists.newArrayList();
        for (P2jLogicalRelationalOperator p2jOp : p2jOpsForJobId(jobId)) {
            if (p2jOp.getPredecessors().size() == 0) {
                result.add(p2jOp.getUid());
            }
        }
        return result;
    }
    
    protected List<String> getSinkNodeIds(String jobId) {
        List<String> result = Lists.newArrayList();
        for (P2jLogicalRelationalOperator p2jOp : p2jOpsForJobId(jobId)) {
            if (p2jOp.getSuccessors().size() == 0) {
                result.add(p2jOp.getUid());
            }
        }
        return result;
    }
    
    protected Long nextP2jId() {
    	Long id = 0l;
    	for (String existing : p2jMap.keySet()) {
    		Long e = Long.valueOf(existing);
    		if (id < e) {
    			id = e;
    		}
    	}
    	id += 1l;
    	return id;
    }
    
    protected void connectUnknownOperators() {        
        for (TezOperator op : tp) {
            Integer count = operatorCounts.get(op.getOperatorKey());
            if (count < 1) {            
                List<TezOperator> preds = tp.getPredecessors(op);
                for (TezOperator pred : preds) {
                    String predJobId = pred.getOperatorKey().toString();
                    List<P2jLogicalRelationalOperator> p2jPreds = getOutBoundaryNodes(predJobId);
                    for (P2jLogicalRelationalOperator p2jPred : p2jPreds) {
                        p2jPred.getSuccessors().addAll(getSourceNodeIds(op.getOperatorKey().toString()));
                    }
                }

                List<TezOperator> succs = tp.getSuccessors(op);
                for (TezOperator succ : succs) {
                    String succJobId = succ.getOperatorKey().toString();
                    List<P2jLogicalRelationalOperator> p2jSuccs = getInBoundaryNodes(succJobId);
                    for (P2jLogicalRelationalOperator p2jSucc : p2jSuccs) {
                        List<String> sinkIds = getSinkNodeIds(op.getOperatorKey().toString());
                        p2jSucc.getPredecessors().addAll(sinkIds);
                        for (String sinkId : sinkIds) {
                            P2jLogicalRelationalOperator sink = p2jMap.get(sinkId);
                            sink.getSuccessors().add(p2jSucc.getUid());
                        }
                    }
                }
            }
        }
    }
    
    protected void appendUnknownOperators(OperatorKey job) {
    	TezOperator op = tp.getOperator(job);
    	
    	// First, get p2jPlan representation of TezOperator's physical plan
    	POJsonAdaptor pja = new POJsonAdaptor(job.toString(), nextP2jId(), op.plan);
    	Map<String, P2jLogicalRelationalOperator> opPlan = pja.getPlan();
    	
    	// Next, add all the new operators to the existing plan
    	p2jMap.putAll(opPlan);    	
    }
}
