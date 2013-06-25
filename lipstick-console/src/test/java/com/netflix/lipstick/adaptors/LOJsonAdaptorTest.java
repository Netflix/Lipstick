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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.LipstickPigServer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.netflix.lipstick.adaptors.LOJsonAdaptor.LogicalExpressionPlanSerializer;
import com.netflix.lipstick.model.operators.P2jLOCogroup;
import com.netflix.lipstick.model.operators.P2jLOFilter;
import com.netflix.lipstick.model.operators.P2jLOJoin;
import com.netflix.lipstick.model.operators.P2jLOLimit;
import com.netflix.lipstick.model.operators.P2jLOLoad;
import com.netflix.lipstick.model.operators.P2jLOSplitOutput;
import com.netflix.lipstick.model.operators.P2jLOStore;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;
import com.netflix.lipstick.util.EzIterable;

public class LOJsonAdaptorTest {
    @Test
    public void generalTest() throws Exception {
        LipstickPigServer lps = new LipstickPigServer("local");
        lps.setBatchOn();
        lps.registerScript("./src/test/resources/test.pig");

        LogicalPlan lp = getLogicalPlan(lps);

        for (Operator op : EzIterable.getIterable(lp.getOperators())) {
            LogicalRelationalOperator lro = (LogicalRelationalOperator) op;
            LOJsonAdaptor adaptor = getAdaptor(lro, lp);
            verifyAdaptor(adaptor, lro);
        }
    }

    private void verifyAdaptor(LOJsonAdaptor adaptor, LogicalRelationalOperator lro) throws FrontendException {
        verifyGenericAdaptor(adaptor, lro);

        if (adaptor instanceof LOLoadJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOLoad);
            verifyLoadAdaptor((LOLoadJsonAdaptor) adaptor, (LOLoad) lro);
        } else if (adaptor instanceof LOStoreJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOStore);
            verifyStoreAdaptor((LOStoreJsonAdaptor) adaptor, (LOStore) lro);
        } else if (adaptor instanceof LOSplitOutputJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOSplitOutput);
            verifySplitAdaptor((LOSplitOutputJsonAdaptor) adaptor, (LOSplitOutput) lro);
        } else if (adaptor instanceof LOJoinJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOJoin);
            verifyJoinAdaptor((LOJoinJsonAdaptor) adaptor, (LOJoin) lro);
        } else if (adaptor instanceof LOCogroupJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOCogroup);
            verifyCogroupAdaptor((LOCogroupJsonAdaptor) adaptor, (LOCogroup) lro);
        } else if (adaptor instanceof LOFilterJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOFilter);
            verifyFilterAdaptor((LOFilterJsonAdaptor) adaptor, (LOFilter) lro);
        } else if (adaptor instanceof LOLimitJsonAdaptor) {
            Assert.assertTrue(lro instanceof LOLimit);
            verifyLimitAdaptor((LOLimitJsonAdaptor) adaptor, (LOLimit) lro);
        }
    }

    private void verifyGenericAdaptor(LOJsonAdaptor adaptor, LogicalRelationalOperator lro) throws FrontendException {
        P2jLogicalRelationalOperator p2j = adaptor.getToP2jOperator();
        if (lro.getSchema() != null) {
            Assert.assertEquals(p2j.getSchemaString(), Util.translateSchema(lro.getSchema()).toString());
        }
        Assert.assertEquals(p2j.getOperator(), lro.getClass().getSimpleName());
        Assert.assertEquals(p2j.getAlias(), lro.getAlias());
        Assert.assertEquals(p2j.getLocation().getLine(), (Integer) lro.getLocation().line());
        Assert.assertEquals(p2j.getLocation().getFilename(), lro.getLocation().file());
    }

    private void verifyLoadAdaptor(LOLoadJsonAdaptor adaptor, LOLoad lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOLoad);
        P2jLOLoad load = (P2jLOLoad) adaptor.getToP2jOperator();
        Assert.assertEquals(load.getStorageLocation(), lro.getFileSpec().getFileName());
        String[] funcList = StringUtils.split(lro.getFileSpec().getFuncName(), ".");
        Assert.assertEquals(load.getStorageFunction(), funcList[funcList.length - 1]);
    }

    private void verifyStoreAdaptor(LOStoreJsonAdaptor adaptor, LOStore lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOStore);
        P2jLOStore store = (P2jLOStore) adaptor.getToP2jOperator();
        Assert.assertEquals(store.getStorageLocation(), lro.getFileSpec().getFileName());
        String[] funcList = StringUtils.split(lro.getFileSpec().getFuncName(), ".");
        Assert.assertEquals(store.getStorageFunction(), funcList[funcList.length - 1]);
    }

    private void verifySplitAdaptor(LOSplitOutputJsonAdaptor adaptor, LOSplitOutput lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOSplitOutput);
        P2jLOSplitOutput split = (P2jLOSplitOutput) adaptor.getToP2jOperator();
        Assert.assertEquals(split.getExpression(), LogicalExpressionPlanSerializer.serialize(lro.getFilterPlan()));
    }

    private void verifyJoinAdaptor(LOJoinJsonAdaptor adaptor, LOJoin lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOJoin);
        P2jLOJoin join = (P2jLOJoin) adaptor.getToP2jOperator();
        Assert.assertNotNull(join.getJoin());
    }

    private void verifyCogroupAdaptor(LOCogroupJsonAdaptor adaptor, LOCogroup lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOCogroup);
        P2jLOCogroup cogroup = (P2jLOCogroup) adaptor.getToP2jOperator();
        Assert.assertNotNull(cogroup.getGroup());
    }

    private void verifyFilterAdaptor(LOFilterJsonAdaptor adaptor, LOFilter lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOFilter);
        P2jLOFilter filter = (P2jLOFilter) adaptor.getToP2jOperator();
        Assert.assertEquals(filter.getExpression(), LogicalExpressionPlanSerializer.serialize(lro.getFilterPlan()));
    }

    private void verifyLimitAdaptor(LOLimitJsonAdaptor adaptor, LOLimit lro) {
        Assert.assertTrue(adaptor.getToP2jOperator() instanceof P2jLOLimit);
        P2jLOLimit limit = (P2jLOLimit) adaptor.getToP2jOperator();
        Assert.assertEquals(limit.getRowLimit(), lro.getLimit());
    }

    private LogicalPlan getLogicalPlan(LipstickPigServer lps) throws Exception {
        Field f = lps.getClass().getSuperclass().getDeclaredField("currDAG");
        f.setAccessible(true);

        Object graph = f.get(lps);
        Method buildPlanMethod = graph.getClass().getDeclaredMethod("buildPlan", String.class);
        buildPlanMethod.setAccessible(true);
        buildPlanMethod.invoke(graph, new Object[] { null });
        Method getPlanMethod = graph.getClass().getMethod("getPlan", String.class);

        return (LogicalPlan) getPlanMethod.invoke(graph, new Object[] { null });
    }

    private LOJsonAdaptor getAdaptor(LogicalRelationalOperator node, LogicalPlan lp) throws FrontendException {
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
