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
package com.netflix.lipstick.adaptors;

import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.lipstick.model.operators.P2jLOJoin;

/**
 * LOJoin to Lipstick model adaptor.
 *
 * @author jmagnusson
 *
 */
public class LOJoinJsonAdaptor extends LOJsonAdaptor {

    /**
     * Instantiate a new LOJoinJsonAdaptor and populate the join expression and join type fields.
     *
     * @param node LOJoin operator to convert to P2jLOJoin.
     * @param lp the LogicalPlan containing node
     * @throws FrontendException
     */
    public LOJoinJsonAdaptor(LOJoin node, LogicalPlan lp) throws FrontendException {
        super(node, new P2jLOJoin(), lp);
        P2jLOJoin join = (P2jLOJoin) p2j;
        join.setJoin(node.getJoinType().toString(), getJoinType(node), getJoinExpressions(node, lp));
    }


    /**
     * Generate a map describing the group expressing for an operator.
     *
     * @param node the LOJoin operator
     * @param lp the LogicalPlan containing node
     * @return a map of alias name to ordered list of fields constituting the join criteria for that alias
     */
    protected Map<String, List<String>> getJoinExpressions(LOJoin node, LogicalPlan lp) {
        Map<String, List<String>> expressions = Maps.newHashMap();
        List<Operator> inputs = node.getInputs(lp);
        MultiMap<Integer, LogicalExpressionPlan> plans = node.getExpressionPlans();
        for (Integer i : plans.keySet()) {
            List<String> planStrings = Lists.newArrayList();
            for (LogicalExpressionPlan p : plans.get(i)) {
                planStrings.add(LogicalExpressionPlanSerializer.serialize(p));
            }
            expressions.put(String.valueOf(((LogicalRelationalOperator) inputs.get(i)).getAlias()), planStrings);
        }
        return expressions;
    }

    /**
     * Get the join type (inner/[full,left,right]outer) for node.
     *
     * @param node the LOJoin operator to inspect
     * @return a string describing the join type
     */
    protected String getJoinType(LOJoin node) {
        boolean[] innerFlags = node.getInnerFlags();
        int sum = 0;
        for (boolean i : innerFlags) {
            if (i) {
                sum++;
            }
        }
        if (sum == innerFlags.length) {
            return "INNER";
        } else if (sum == 0) {
            return "FULL OUTER";
        } else if (innerFlags[0]) {
            return "LEFT OUTER";
        }
        return "RIGHT OUTER";
    }

}
