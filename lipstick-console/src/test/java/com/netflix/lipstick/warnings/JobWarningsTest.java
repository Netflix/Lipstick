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
package com.netflix.lipstick.warnings;

import static org.testng.Assert.*;
import org.testng.annotations.Test;
import static org.mockito.Mockito.*;

import java.util.List;
import org.apache.pig.tools.pigstats.JobStats;
import com.google.common.collect.Lists;

public class JobWarningsTest {

    @Test
    public void testFindSkewedReducersNotEnoughTasks() throws Exception {
        JobWarnings jw = new JobWarnings();
        List<JobWarnings.ReducerDuration> reducerTimes = Lists.newLinkedList();
        List<String> taskIds;
        JobWarnings.ReducerDuration rd;

        for (int i = 0; i < JobWarnings.MIN_REDUCERS_FOR_SKEW; i++) {
            rd = new JobWarnings.ReducerDuration("Some Job", i);
            reducerTimes.add(rd);
            taskIds = jw.findSkewedReducers(reducerTimes);
            assertEquals(0, taskIds.size());
        }

        /* Now that we've trie the most number of reducers w/o checking, lets
           try again with a duration value that is obviously skewed. */
        rd = new JobWarnings.ReducerDuration("Some Job", 10000);
        reducerTimes.add(rd);
        taskIds = jw.findSkewedReducers(reducerTimes);
        assertEquals(1, taskIds.size());
    }

    @Test
    public void testFindSkewedReducersSkewedReducersPresent() throws Exception {
        List<JobWarnings.ReducerDuration> reducerTimes = Lists.newLinkedList();
        reducerTimes.add(new JobWarnings.ReducerDuration("task_201310241542_0008_r_000005", (1382638023848l)));
        reducerTimes.add(new JobWarnings.ReducerDuration("task_201310241542_0008_r_000006", (1382638023848l)));
        reducerTimes.add(new JobWarnings.ReducerDuration("task_201310241542_0008_r_000007", (1382638023849l)));
        reducerTimes.add(new JobWarnings.ReducerDuration("task_201310241542_0008_r_000009", (1382638023849l)));
        // This should be detected as a skew
        reducerTimes.add(new JobWarnings.ReducerDuration("task_201310241542_0008_r_000008", (138263802384800l)));

        JobWarnings jw = new JobWarnings();
        List<String> taskIds = jw.findSkewedReducers(reducerTimes);
        assertEquals(1, taskIds.size());
        assertEquals("task_201310241542_0008_r_000008", taskIds.get(0));
    }

}

