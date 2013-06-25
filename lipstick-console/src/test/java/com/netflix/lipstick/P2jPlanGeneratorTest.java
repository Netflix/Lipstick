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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pig.LipstickPigServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.operators.P2jLOCogroup;
import com.netflix.lipstick.model.operators.P2jLOFilter;
import com.netflix.lipstick.model.operators.P2jLOJoin;
import com.netflix.lipstick.model.operators.P2jLOLimit;
import com.netflix.lipstick.model.operators.P2jLOLoad;
import com.netflix.lipstick.model.operators.P2jLOStore;
import com.netflix.lipstick.model.operators.P2jLogicalRelationalOperator;
import com.netflix.lipstick.test.util.Util;

public class P2jPlanGeneratorTest {
    @Test
    public void generalTest() throws Exception {
        LipstickPigServer lps = new LipstickPigServer("local");
        lps.setBatchOn();
        lps.registerScript("./src/test/resources/test.pig");

        P2jPlanGenerator opg = new P2jPlanGenerator(lps.getLP(null));
        P2jPlan plan = opg.getP2jPlan();

        // Build a map of scope to id from the built plan
        Map<String, String> scopeToIdMap = new HashMap<String, String>();

        for (String scope : plan.getPlan().keySet()) {
            scopeToIdMap.put(scope, getIdentifier(plan.getPlan().get(scope)));
        }

        // Container for all expected P2j objects
        Set<P2jLogicalRelationalOperator> expectedOps = new HashSet<P2jLogicalRelationalOperator>();

        // Add all expected P2jLOLoad objects
        P2jLOLoad load1 = new P2jLOLoad();
        load1.setAlias("tiny");
        load1.setSuccessors(Lists.newArrayList("tiny_colors", "tiny_colors_cogrp", "tiny_colors_join"));
        P2jLOLoad load2 = new P2jLOLoad();
        load2.setAlias("colors");
        load2.setSuccessors(Lists.newArrayList("tiny_colors", "colors_filtered", "tiny_colors_cogrp", "tiny_colors_join"));
        P2jLOLoad load3 = new P2jLOLoad();
        load3.setAlias("colors2");
        load3.setSuccessors(Lists.newArrayList("tiny_colors_cogrp", "tiny_colors_join"));
        P2jLOLoad load4 = new P2jLOLoad();
        load4.setAlias("colors3");
        load4.setSuccessors(Lists.newArrayList("tiny_colors_join"));

        expectedOps.addAll(Lists.newArrayList(load1, load2, load3, load4));

        // Add all expected P2jLOStore objects
        // Aliases are lost, so use storageLocation instead
        P2jLOStore store1 = new P2jLOStore();
        store1.setStorageLocation("file://" + System.getProperty("user.dir") + "/test_out_cogrp");
        store1.setPredecessors(Lists.newArrayList("out"));
        P2jLOStore store2 = new P2jLOStore();
        store2.setStorageLocation("file://" + System.getProperty("user.dir") + "/test_out_join");
        store2.setPredecessors(Lists.newArrayList("tiny_colors_join"));
        P2jLOStore store3 = new P2jLOStore();
        store3.setStorageLocation("file://" + System.getProperty("user.dir") + "/test_out_tiny_colors");
        store3.setPredecessors(Lists.newArrayList("tiny_colors"));

        expectedOps.addAll(Lists.newArrayList(store1, store2, store3));

        // Add all expected P2jLOJoin objects
        P2jLOJoin join1 = new P2jLOJoin();
        join1.setAlias("tiny_colors");
        join1.setPredecessors(Lists.newArrayList("tiny", "colors"));
        join1.setSuccessors(Lists.newArrayList("file://" + System.getProperty("user.dir") + "/test_out_tiny_colors"));
        P2jLOJoin join2 = new P2jLOJoin();
        join2.setAlias("tiny_colors_join");
        join2.setPredecessors(Lists.newArrayList("tiny", "colors", "colors2", "colors3"));
        join2.setSuccessors(Lists.newArrayList("file://" + System.getProperty("user.dir") + "/test_out_join"));

        expectedOps.addAll(Lists.newArrayList(join1, join2));

        // Add all expected P2jLOFilter objects
        P2jLOFilter filter1 = new P2jLOFilter();
        filter1.setAlias("colors_filtered");
        filter1.setPredecessors(Lists.newArrayList("colors"));

        expectedOps.add(filter1);

        // Add all expected P2jLOCogroup objects
        P2jLOCogroup cogroup1 = new P2jLOCogroup();
        cogroup1.setAlias("tiny_colors_cogrp");
        cogroup1.setPredecessors(Lists.newArrayList("tiny", "colors", "colors2"));
        cogroup1.setSuccessors(Lists.newArrayList("out"));

        expectedOps.add(cogroup1);

        // Add all expected P2jLOLimit objects
        P2jLOLimit limit1 = new P2jLOLimit();
        limit1.setAlias("out");
        limit1.setPredecessors(Lists.newArrayList("tiny_colors_cogrp"));
        limit1.setSuccessors(Lists.newArrayList("file://" + System.getProperty("user.dir") + "/test_out_cogrp"));

        expectedOps.add(limit1);

        // For each op, ensure the aliases and all predecessors/successors match
        for (String scope : plan.getPlan().keySet()) {
            P2jLogicalRelationalOperator actualOp = plan.getPlan().get(scope);

            String actualId = getIdentifier(actualOp);

            P2jLogicalRelationalOperator matchedOp = null;
            for (P2jLogicalRelationalOperator expectedOp : expectedOps) {
                String expectedId = getIdentifier(expectedOp);
                if (actualId.equals(expectedId)) {
                    matchedOp = expectedOp;
                    // Compare classes
                    Assert.assertEquals(actualOp.getClass(), expectedOp.getClass());

                    // Compare predecessors
                    Set<String> actualPredecessorAliases = Sets.newHashSet();
                    for (String predScope : actualOp.getPredecessors()) {
                        actualPredecessorAliases.add(scopeToIdMap.get(predScope));
                    }

                    SetView<String> predDiff = Util.safeDiffSets(actualPredecessorAliases, Util.safeNewSet(expectedOp.getPredecessors()));
                    Assert.assertEquals(predDiff.size(), 0);

                    // Compare successors
                    Set<String> actualSuccessorAliases = Sets.newHashSet();
                    for (String succScope : actualOp.getSuccessors()) {
                        actualSuccessorAliases.add(scopeToIdMap.get(succScope));
                    }
                    SetView<String> succDiff = Util.safeDiffSets(actualSuccessorAliases, Util.safeNewSet(expectedOp.getSuccessors()));
                    Assert.assertEquals(succDiff.size(), 0);

                    break;
                }
            }

            Assert.assertNotNull(matchedOp);
            expectedOps.remove(matchedOp);
        }

        Assert.assertEquals(expectedOps.size(), 0);
    }

    private String getIdentifier(P2jLogicalRelationalOperator op) {
        return (op instanceof P2jLOStore) ? ((P2jLOStore) op).getStorageLocation() : op.getAlias();
    }
}
