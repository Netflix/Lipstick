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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import com.netflix.lipstick.model.operators.P2jLOFilter;

/**
 * LOFilter to Lipstick model adaptor.
 *
 * @author jmagnusson
 *
 */
public class LOFilterJsonAdaptor extends LOJsonAdaptor {

    /**
     * Instantiate a new LOFilterJsonAdaptor and populate the filter expression field.
     *
     * @param node LOFilterJsonAdaptor operator to convert to P2jLOCogroup.
     * @param lp the LogicalPlan containing node
     * @throws FrontendException
     */
    public LOFilterJsonAdaptor(LOFilter node, LogicalPlan lp) throws FrontendException {
        super(node, new P2jLOFilter(), lp);
        P2jLOFilter filter = (P2jLOFilter) p2j;
        filter.setExpression(LogicalExpressionPlanSerializer.serialize(node.getFilterPlan()));
    }

}
