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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.lipstick.adaptors.LOCogroupJsonAdaptor;
import com.netflix.lipstick.adaptors.LOFilterJsonAdaptor;
import com.netflix.lipstick.adaptors.LOJoinJsonAdaptor;
import com.netflix.lipstick.adaptors.LOJsonAdaptor;
import com.netflix.lipstick.adaptors.LOLimitJsonAdaptor;
import com.netflix.lipstick.adaptors.LOLoadJsonAdaptor;
import com.netflix.lipstick.adaptors.LOSplitOutputJsonAdaptor;
import com.netflix.lipstick.adaptors.LOStoreJsonAdaptor;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

/**
 * Generate a P2jPlan given a LogicalPlan.
 *
 * @author jmagnusson
 */
public class P2jPlanGenerator {

    /** Mapping of logical operator to p2j operator id. */
    protected Map<Operator, String> reverseMap;

    /** The p2j plan. */
    protected P2jPlan p2jPlan;

    /**
     * Instantiates a new p2j plan generator. Generates reverseMap and p2jPlan.
     *
     * @param lp
     *            the logical plan
     * @throws FrontendException
     *             the frontend exception
     */
    public P2jPlanGenerator(LogicalPlan lp) throws FrontendException {
        reverseMap = generateReverseMap(lp);
        Map<String, P2jLogicalRelationalOperator> nodeMap = Maps.newHashMap();
        for (Entry<Operator, String> entry : reverseMap.entrySet()) {
            nodeMap.put(entry.getValue(), convertNodeToP2j((LogicalRelationalOperator) entry.getKey(), lp, reverseMap));
        }
        p2jPlan = new P2jPlan(nodeMap);
    }

    /**
     * Gets the reverse map of logical operators to p2j operator ids.
     *
     * @return the reverse map
     */
    public Map<Operator, String> getReverseMap() {
        return reverseMap;
    }

    /**
     * Gets the p2j plan.
     *
     * @return the p2j plan
     */
    public P2jPlan getP2jPlan() {
        return p2jPlan;
    }

    /**
     * Generate reverse mapping of logical operators to p2j operator id.
     *
     * @param lp
     *            the logical plan
     * @return the reverse map
     */
    protected Map<Operator, String> generateReverseMap(LogicalPlan lp) {
        Map<Operator, String> map = Maps.newLinkedHashMap();
        Integer counter = 1;
        Iterator<Operator> ops = lp.getOperators();
        while (ops.hasNext()) {
            map.put(ops.next(), counter.toString());
            counter++;
        }
        return map;
    }

    /**
     * Given a list of logical operators, return the list of p2j opeartor ids.
     * they represent
     *
     * @param nodes
     *            list of logical operators
     * @param reverseMap
     *            mapping of logical operator to p2j operator ids
     * @return the list of p2j operator ids
     */
    protected List<String> generateP2jIdList(List<Operator> nodes, Map<Operator, String> reverseMap) {
        List<String> chain = Lists.newLinkedList();
        if (nodes != null && !nodes.isEmpty()) {
            for (Operator i : nodes) {
                chain.add(reverseMap.get(i));
            }
        }
        return chain;
    }

    /**
     * Convert a LogicalRelationalOperator to a P2jLogicalRelationalOperator.
     *
     * @param node
     *            the LogicalRelationalOperator to convert
     * @param lp
     *            the LogicalPlan
     * @param reverseMap
     *            the mapping of logical operator to
     *            P2jLogicalRelationalOperator id
     * @return a P2jLogicalRelationalOperator representing the node passed in
     * @throws FrontendException
     *
     */
    protected P2jLogicalRelationalOperator convertNodeToP2j(LogicalRelationalOperator node,
                                                            LogicalPlan lp,
                                                            Map<Operator, String> reverseMap) throws FrontendException {
        P2jLogicalRelationalOperator p2jNode = convertNodeToAdaptor(node, lp).getToP2jOperator();
        p2jNode.setPredecessors(generateP2jIdList(lp.getPredecessors(node), reverseMap));
        p2jNode.setSuccessors(generateP2jIdList(lp.getSuccessors(node), reverseMap));
        p2jNode.setUid(reverseMap.get(node));
        return p2jNode;
    }

    /**
     * Convert a LogicalRelationalOperator to an LOJsonAdaptor.
     *
     * @param node
     *            the LogicalRelationalOperator to convert
     * @param lp
     *            the LogicalPlan containing node
     * @return a LOJsonAdaptor representing node
     * @throws FrontendException
     */
    protected LOJsonAdaptor convertNodeToAdaptor(LogicalRelationalOperator node, LogicalPlan lp)
            throws FrontendException {
        if (node instanceof LOLoad) {
            return new LOLoadJsonAdaptor((LOLoad) node, lp);
        } else if (node instanceof LOStore) {
            return new LOStoreJsonAdaptor((LOStore) node, lp);
        } else if (node instanceof LOSplitOutput) {
            return new LOSplitOutputJsonAdaptor((LOSplitOutput) node, lp);
        } else if (node instanceof LOJoin) {
            return new LOJoinJsonAdaptor((LOJoin) node, lp);
        } else if (node instanceof LOCogroup) {
            return new LOCogroupJsonAdaptor((LOCogroup) node, lp);
        } else if (node instanceof LOFilter) {
            return new LOFilterJsonAdaptor((LOFilter) node, lp);
        } else if (node instanceof LOLimit) {
            return new LOLimitJsonAdaptor((LOLimit) node, lp);
        }
        return new LOJsonAdaptor(node, lp);
    }

}
