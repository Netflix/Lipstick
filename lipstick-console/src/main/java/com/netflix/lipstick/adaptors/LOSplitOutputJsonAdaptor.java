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
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import com.netflix.lipstick.model.operators.P2jLOSplitOutput;

/**
 * LOSplit to Lipstick model adaptor.
 *
 * @author jmagnusson
 */
public class LOSplitOutputJsonAdaptor extends LOJsonAdaptor {

    /**
     * Initializes a new LOSplitOutputJsonAdaptor and additionally sets the
     * filter expression on the P2jLOSplitOutput object that is created.
     *
     * @param node the LOSplitOutput to adapt to P2jLOSplitOutput
     * @param lp the logical plan containing node
     * @throws FrontendException
     */
    public LOSplitOutputJsonAdaptor(LOSplitOutput node, LogicalPlan lp) throws FrontendException {
        super(node, new P2jLOSplitOutput(), lp);
        ((P2jLOSplitOutput) p2j).setExpression(LogicalExpressionPlanSerializer.serialize(node.getFilterPlan()));
    }

}
