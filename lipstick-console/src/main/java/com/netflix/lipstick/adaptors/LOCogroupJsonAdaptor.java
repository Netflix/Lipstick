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
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.lipstick.model.operators.P2jLOCogroup;

/**
 * Adaptor for LOCogroup to Lipstick model.
 *
 * @author jmagnusson
 *
 */
public class LOCogroupJsonAdaptor extends LOJsonAdaptor {

    /**
     * Instantiate a new LOCogroupJsonAdaptor and populate the group type and group expression field.
     *
     * @param node LOCogroup operator to convert to P2jLOCogroup.
     * @param lp the LogicalPlan containing node
     * @throws FrontendException
     */
    public LOCogroupJsonAdaptor(LOCogroup node, LogicalPlan lp) throws FrontendException {
        super(node, new P2jLOCogroup(), lp);
        P2jLOCogroup cogroup = (P2jLOCogroup) p2j;
        cogroup.setGroup(node.getGroupType().toString(), getGroupType(node), getGroupExpressions(node, lp));
    }

    /**
     * Get the group type (inner/outer) for node.
     *
     * @param node the LOCogroup operator to inspect
     * @return a string describing the group type
     */
    protected String getGroupType(LOCogroup node) {
        String ret = "INNER";
        for (boolean flag : node.getInner()) {
            if (!flag) {
                ret = "OUTER";
                break;
            }
        }
        return ret;
    }

    /**
     * Generate a map describing the group expressing for an operator.
     *
     * @param node the LOCogroup operator
     * @param lp the LogicalPlan containing node
     * @return a map of alias name to ordered list of fields constituting the group criteria for that alias
     */
    protected Map<String, List<String>> getGroupExpressions(LOCogroup node, LogicalPlan lp) {
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

}
