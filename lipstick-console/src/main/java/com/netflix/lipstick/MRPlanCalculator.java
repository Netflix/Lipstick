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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.OriginalLocation;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMultiQueryPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLOStore;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

/**
 *
 * Assigns map/reduce stages to all operators in a p2jPlan.
 *
 * @author jmagnusson
 *
 */
public class MRPlanCalculator {
    private static final Log LOG = LogFactory.getLog(MRPlanCalculator.class);

    protected MROperPlan mrp;
    protected Map<PhysicalOperator, Operator> phy2LogMap;
    protected Map<String, P2jLogicalRelationalOperator> p2jMap;
    protected P2jPlan p2jPlan;
    protected Map<Operator, String> reverseMap;
    protected Map<String, Operator> locationMap;

    /**
     * Possible map/reduce job phases.
     *
     */
    public static enum MRStepType {
        MAPPER, REDUCER, COMBINER, UNKNOWN
    }

    /**
     * Instantiates a new map/reduce plan calculator. Assigns map/reduce plans
     * to p2jPlan.
     *
     * @param p2jPlan
     *            the P2jPlan
     * @param mrp
     *            the MROperPlan
     * @param phy2LogMap
     *            physical to logical operator map
     * @param reverseMap
     *            reverse map of logical operator to operator uid
     */
    public MRPlanCalculator(P2jPlan p2jPlan,
                            MROperPlan mrp,
                            Map<PhysicalOperator, Operator> phy2LogMap,
                            Map<Operator, String> reverseMap) {
        this.mrp = mrp;
        this.phy2LogMap = phy2LogMap;
        this.p2jMap = p2jPlan.getPlan();
        this.p2jPlan = p2jPlan;
        this.reverseMap = reverseMap;
        this.locationMap = generateLocationMap();
        p2jPlan.setPlan(assignMRStagesToNodes());
    }

    /**
     * Get the P2jPlan with map/reduce jobs assigned.
     *
     * @return p2jPlan with map/reduce jobs assigned
     */
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

    /**
     * Assign map/reduce jobs to P2jLogicalRelationalOperators, first by
     * iterating through the MROperPlan and mapping physical operators to
     * P2jLogicalRelationalOperators. Finally assign operators that could not be
     * mapped by using information from the unassigned operators successors and
     * predecessors via assignMRStagesToUnknownNodes.
     *
     * @return the p2jMap with map/reduce jobs assigned to all nodes
     */
    protected Map<String, P2jLogicalRelationalOperator> assignMRStagesToNodes() {
        for (MapReduceOper job : mrp) {
            String jid = job.getOperatorKey().toString();
            assignMRStagesToPlan(job.mapPlan, jid, MRStepType.MAPPER);
            assignMRStagesToPlan(job.reducePlan, jid, MRStepType.REDUCER);
            assignMRStagesToPlan(job.combinePlan, jid, MRStepType.COMBINER);
        }
        // assign to the operators that were not assigned previously
        assignMRStagesToUnknownNodes();
        return p2jMap;
    }

    /**
     * Return the P2jLogicalRelationalOperator associated with a physical store
     * operator.
     *
     * @param pop
     *            the physical store operator
     * @return the P2jLogicalRelationalOperator associated with pop
     */
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

    /**
     * Assign map/reduce job and step to logical operators identifiable through
     * a physical plan.
     *
     * @param pp
     *            the PhysicalPlan
     * @param jid
     *            a string representing the M/R job id of the plan
     * @param stepType
     *            map/reduce phase for the physical plan
     */
    protected void assignMRStagesToPlan(PhysicalPlan pp, String jid, MRStepType stepType) {
        for (PhysicalOperator pop : pp) {
            assignMRStage(pop, jid, stepType);
        }
    }

    /**
     * Given a physical operator, attempts to map it to a logical operator. If a
     * suitable mapping can be found, assign map reduce phase "jid, stepType" to
     * the logical operator.
     *
     * @param pop
     *            the physical operator
     * @param jid
     *            the map/reduce job id
     * @param stepType
     *            the map/reduce step (MAPPER, REDUCER, COMBINER, UNKNOWN)
     */
    protected void assignMRStage(PhysicalOperator pop, String jid, MRStepType stepType) {

        // special cases - find other operators inside these that need to be
        // assigned

        if (pop instanceof POJoinPackage) {
            POJoinPackage jpop = (POJoinPackage) pop;
            assignMRStage(jpop.getForEach(), jid, stepType);
        } else if (pop instanceof POLocalRearrange) {
            for (PhysicalPlan ipl : ((POLocalRearrange) pop).getPlans()) {
                assignMRStagesToPlan(ipl, jid, stepType);
            }
        } else if (pop instanceof PODemux) {
            for (PhysicalPlan ipl : ((PODemux) pop).getPlans()) {
                assignMRStagesToPlan(ipl, jid, stepType);
            }
        } else if (pop instanceof POPreCombinerLocalRearrange) {
            for (PhysicalPlan ipl : ((POPreCombinerLocalRearrange) pop).getPlans()) {
                assignMRStagesToPlan(ipl, jid, stepType);
            }
        } else if (pop instanceof POMultiQueryPackage) {
            for (PhysicalOperator iop : ((POMultiQueryPackage) pop).getPackages()) {
                assignMRStage(iop, jid, stepType);
            }
        } else if (pop instanceof POSplit) {
            for (PhysicalPlan ipl : ((POSplit) pop).getPlans()) {
                assignMRStagesToPlan(ipl, jid, stepType);
            }
        }

        String stepTypeString = stepType.toString();

        if (pop instanceof POStore) {
            P2jLogicalRelationalOperator node = getOpForStore((POStore) pop);
            if (node != null) {
                node.setMapReduce(jid, stepTypeString);
                return;
            }
        } else if (phy2LogMap.containsKey(pop) && reverseMap.containsKey(phy2LogMap.get(pop))) {
            String nodeId = reverseMap.get(phy2LogMap.get(pop));
            P2jLogicalRelationalOperator node = p2jMap.get(nodeId);
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
                        if (node.getOperator().equalsIgnoreCase("LOJoin")
                            || node.getOperator().equalsIgnoreCase("LOGroup")) {
                            stepTypeString = MRStepType.REDUCER.toString();
                        }
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

    /**
     * Assign map/reduce jobs to previously unassigned
     * P2jLogicalRelationalOperators in the plan, using information from the
     * operator's successors and predecessors.
     */
    protected void assignMRStagesToUnknownNodes() {
        for (P2jLogicalRelationalOperator node : p2jMap.values()) {
            if (node.getMapReduce() == null || node.getMapReduce().getJobId() == null) {
                String jobId = resolveJobForNode(node);
                if (jobId != null) {
                    node.setMapReduce(jobId, MRStepType.UNKNOWN.toString());
                }
            }
        }
    }

    /**
     * Attempts to determine a map/reduce job that is responsible for a
     * P2jLogicalOperator, via the job information from the operator's
     * predecessors and successors.
     *
     * @param node
     *            the P2jLogicalRelationalOperator
     * @return a String representing the map/reduce job id
     */
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

}
