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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
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
    protected Map<String, P2jLogicalRelationalOperator> p2jMap;

    
    public TezPlanCalculator(P2jPlan p2jPlan, TezOperPlan tp) {
        this.tp = tp;
        this.p2jMap = p2jPlan.getPlan();
        this.p2jPlan = p2jPlan;
        p2jMap.clear(); // We're going to use Physical Operators only; remove what's there
        p2jPlan.setPlan(assignVerticesToNodes());        
    }

    public P2jPlan getP2jPlan() {
        return p2jPlan;
    }
    
    protected Map<String, P2jLogicalRelationalOperator> assignVerticesToNodes() {
        
        for (TezOperator job : tp) {
            assignPhysicalOperators(job);
        }
        
        connectPhysicalOperators();

        p2jPlan.setPlan(p2jMap);
        return p2jMap;
    }
        
    protected List<P2jLogicalRelationalOperator> p2jOpsForJobId(String jobId) {
    	List<P2jLogicalRelationalOperator> result = Lists.newArrayList();
    	for (P2jLogicalRelationalOperator op : p2jMap.values()) {
    		if (op.getMapReduce() != null && op.getMapReduce().getJobId() != null) {
    			if (op.getMapReduce().getJobId().equals(jobId)) {
    				result.add(op);
    			}
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
    	            if (succ.getMapReduce() == null || succ.getMapReduce().getJobId() == null || !succ.getMapReduce().getJobId().equals(jobId)) {
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
    	             if (pred.getMapReduce() == null || pred.getMapReduce().getJobId() == null || !pred.getMapReduce().getJobId().equals(jobId)) {
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
    
    protected void connectPhysicalOperators() {
        for (TezOperator op : tp) {
            List<TezOperator> preds = tp.getPredecessors(op);
            if (preds != null) {
                for (TezOperator pred : preds) {
                    String predJobId = pred.getOperatorKey().toString();
                    List<P2jLogicalRelationalOperator> p2jPreds = getOutBoundaryNodes(predJobId);
                    for (P2jLogicalRelationalOperator p2jPred : p2jPreds) {
                        List<String> sourceNodeIds = getSourceNodeIds(op.getOperatorKey().toString()); 
                        for (String sourceNodeId : sourceNodeIds) {
                            List<String> successors = p2jPred.getSuccessors();
                            if (!successors.contains(sourceNodeId)) {
                                successors.add(sourceNodeId);
                            }
                        }
                    }
                }

                List<TezOperator> succs = tp.getSuccessors(op);
                if (succs != null) {
                    String from = op.getOperatorKey().toString();
                    List<String> storePreds = getSinkNodeIds(from);
                    for (TezOperator succ : succs) {
                        // Handle UnionOptimization
                        if (succ.isVertexGroup()) {
                            // In this case there are more ops, other than a store
                            // after the VertexGroup - simply bypass the VertexGroup
                            // and connect operators accordingly.
                            List<TezOperator> realSuccs = tp.getSuccessors(succ);
                            if (realSuccs != null && realSuccs.size() > 0) {
                                TezOperator realSucc = tp.getSuccessors(succ).get(0);
                                String to = realSucc.getOperatorKey().toString();
                                connect(from, to);
                            } else {
                                // In this case there are no more ops after the union -
                                // special case of storing immediately after a union.
                                // Here we get the store referenced and add it to the 
                                // associated vertex.
                                POStore store = succ.getVertexGroupInfo().getStore();
                                Long storeId = nextP2jId();                                    
                                P2jLogicalRelationalOperator p2jStore = POJsonAdaptor.translateOp(
                                        from, storeId.toString(), store, new ArrayList<String>(), storePreds);
                                boolean validStore = false;
                                for (String storePred : storePreds) {
                                    P2jLogicalRelationalOperator p2jStorePred = p2jMap.get(storePred);
                                    if (
                                            !p2jStorePred.getOperator().equals(p2jStore.getOperator()) && 
                                            !p2jStorePred.getSuccessors().contains(p2jStore.getUid())) {
                                        validStore = true;
                                        p2jStorePred.getSuccessors().add(p2jStore.getUid());                                            
                                    } 
                                }
                                if (validStore) {
                                    p2jMap.put(storeId.toString(), p2jStore);
                                }
                            }
                        } else {
                            // Simple case, no unions
                            String to = succ.getOperatorKey().toString();
                            connect(from, to);
                        }
                    }
                }
            }
        }
    }
    
    protected void connect(String fromId, String toId) {
        List<P2jLogicalRelationalOperator> p2jSuccs = getInBoundaryNodes(toId);
        List<String> sinkIds = getSinkNodeIds(fromId);
        for (P2jLogicalRelationalOperator p2jSucc : p2jSuccs) {
            for (String sinkId : sinkIds) {
                P2jLogicalRelationalOperator sink = p2jMap.get(sinkId);
                if (!sink.getSuccessors().contains(p2jSucc.getUid())) {
                    sink.getSuccessors().add(p2jSucc.getUid());
                } 
            }
        }
    }
    
    protected void assignPhysicalOperators(TezOperator job) {    	
    	// First, get p2jPlan representation of TezOperator's physical plan
    	POJsonAdaptor pja = new POJsonAdaptor(job.getOperatorKey().toString(), nextP2jId(), job.plan);
    	Map<String, P2jLogicalRelationalOperator> opPlan = pja.getPlan();
    	
    	// Next, add all the new operators to the existing plan
    	p2jMap.putAll(opPlan);    	
    }
}
