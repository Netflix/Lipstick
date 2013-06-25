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

import org.apache.commons.lang.StringUtils;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import com.netflix.lipstick.model.operators.P2jLOStore;

/**
 * LOStore to Lipstick model adaptor.
 *
 * @author jmagnusson
 *
 */
public class LOStoreJsonAdaptor extends LOJsonAdaptor {

    /**
     * Initializes a new LOStoreJsonAdaptor and additionally sets the storage
     * function and location on the P2jLOStore object being created.
     *
     * @param node the LOStore object to convert to P2jLOStore
     * @param lp the logical plan containing node
     * @throws FrontendException
     */
    public LOStoreJsonAdaptor(LOStore node, LogicalPlan lp) throws FrontendException {
        super(node, new P2jLOStore(), lp);
        P2jLOStore store = (P2jLOStore) p2j;
        store.setStorageLocation(node.getFileSpec().getFileName());
        String[] funcList = StringUtils.split(node.getFileSpec().getFuncName(), ".");
        store.setStorageFunction(funcList[funcList.length - 1]);
    }

}
