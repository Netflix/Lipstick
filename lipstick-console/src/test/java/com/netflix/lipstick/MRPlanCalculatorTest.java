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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.pig.LipstickPigServer;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.netflix.lipstick.MRPlanCalculator.MRStepType;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLOStore;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;

public class MRPlanCalculatorTest {
    @Test
    public void generalTest() throws Exception {
        LipstickPigServer lps = new LipstickPigServer("local");
        lps.setBatchOn();
        lps.registerScript("./src/test/resources/test.pig");

        P2jPlanGenerator opg = getOpPlanGenerator(lps);

        MROperPlan plan = getMROperPlan(lps);
        Map<PhysicalOperator, Operator> p2lMap = getP2lMap(lps);

        MRPlanCalculator opCalc = new MRPlanCalculator(opg.getP2jPlan(), plan, p2lMap, opg.getReverseMap());

        P2jPlan opPlan = opCalc.getP2jPlan();

        Map<String, MRStepType> expectedIdToStepTypeMap = new HashMap<String, MRStepType>();
        expectedIdToStepTypeMap.put("tiny", MRStepType.MAPPER);
        expectedIdToStepTypeMap.put("colors", MRStepType.MAPPER);
        expectedIdToStepTypeMap.put("colors2", MRStepType.MAPPER);
        expectedIdToStepTypeMap.put("colors3", MRStepType.MAPPER);
        expectedIdToStepTypeMap.put("file://" + System.getProperty("user.dir") + "/test_out_cogrp", MRStepType.REDUCER);
        expectedIdToStepTypeMap.put("file://" + System.getProperty("user.dir") + "/test_out_join", MRStepType.REDUCER);
        expectedIdToStepTypeMap.put("file://" + System.getProperty("user.dir") + "/test_out_tiny_colors", MRStepType.REDUCER);
        expectedIdToStepTypeMap.put("tiny_colors", MRStepType.REDUCER);
        expectedIdToStepTypeMap.put("tiny_colors_join", MRStepType.REDUCER);
        expectedIdToStepTypeMap.put("colors_filtered", MRStepType.UNKNOWN);
        expectedIdToStepTypeMap.put("tiny_colors_cogrp", MRStepType.MAPPER);
        expectedIdToStepTypeMap.put("out", MRStepType.REDUCER);

        for (String scope : opPlan.getPlan().keySet()) {
            P2jLogicalRelationalOperator actualOp = opPlan.getPlan().get(scope);

            String actualId = getIdentifier(actualOp);

            String actualStepType = actualOp.getMapReduce().getStepType();
            String expectedStepType = expectedIdToStepTypeMap.get(actualId).toString();

            Assert.assertEquals(actualStepType, expectedStepType);
        }
    }

    private P2jPlanGenerator getOpPlanGenerator(LipstickPigServer lps) throws Exception {
        return new P2jPlanGenerator(lps.getLP(null));
    }

    private Map<PhysicalOperator, Operator> getP2lMap(LipstickPigServer lps) throws Exception {
        HExecutionEngine he = new HExecutionEngine(lps.getPigContext());
        he.compile(getLogicalPlan(lps), null);

        Map<Operator, PhysicalOperator> l2pMap = he.getLogToPhyMap();
        Map<PhysicalOperator, Operator> p2lMap = Maps.newHashMap();
        for (Entry<Operator, PhysicalOperator> i : l2pMap.entrySet()) {
            p2lMap.put(i.getValue(), i.getKey());
        }

        return p2lMap;
    }

    private MROperPlan getMROperPlan(LipstickPigServer lps) throws Exception {
        HExecutionEngine he = new HExecutionEngine(lps.getPigContext());
        PhysicalPlan pp = he.compile(getLogicalPlan(lps), null);

        MRCompiler mrc = new MRCompiler(pp, lps.getPigContext());
        mrc.compile();
        return mrc.getMRPlan();
    }

    private LogicalPlan getLogicalPlan(LipstickPigServer lps) throws Exception {
        Field f = lps.getClass().getSuperclass().getDeclaredField("currDAG");
        f.setAccessible(true);

        Object graph = f.get(lps);
        Method parseQueryMethod = graph.getClass().getDeclaredMethod("parseQuery");
        parseQueryMethod.setAccessible(true);
        parseQueryMethod.invoke(graph);
        
        Method buildPlanMethod = graph.getClass().getDeclaredMethod("buildPlan", String.class);
        buildPlanMethod.setAccessible(true);
        buildPlanMethod.invoke(graph, new Object[] { null });
        
        Method compilePlanMethod = graph.getClass().getDeclaredMethod("compile");
        compilePlanMethod.setAccessible(true);
        compilePlanMethod.invoke(graph);
        
        Method getPlanMethod = graph.getClass().getMethod("getPlan", String.class);

        return (LogicalPlan) getPlanMethod.invoke(graph, new Object[] { null });
    }

    private String getIdentifier(P2jLogicalRelationalOperator op) {
        return (op instanceof P2jLOStore) ? ((P2jLOStore) op).getStorageLocation() : op.getAlias();
    }
}
