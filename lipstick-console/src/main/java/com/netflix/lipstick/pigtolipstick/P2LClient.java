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
package com.netflix.lipstick.pigtolipstick;

import org.apache.pig.impl.PigContext;
import org.apache.pig.LipstickPigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;

import com.netflix.lipstick.P2jPlanGenerator;

/**
 * Interface to manage translation of Pig to Lipstick model objects.
 *
 * @author nbates
 *
 */
public interface P2LClient {
    /**
     * Creates a Serializable Internal Plan from a MROperPlan.
     * Some implementations assume setPlanGenerators method has already been called.
     *
     * @param plan
     */
    void createPlan(MROperPlan plan);

    /**
     * Sets the unoptimized and optimized plan generators.
     * Some implementations assume this is called before createPlan.
     *
     * @param unopPlanGenerator
     * @param opPlanGenerator
     */
    void setPlanGenerators(P2jPlanGenerator unopPlanGenerator, P2jPlanGenerator opPlanGenerator);

    /**
     * Sets the LipstickPigServer which is used to provide context information.
     *
     * @param ps
     */
    void setPigServer(LipstickPigServer ps);

    /**
     * Sets the PigContext which is used to provide context information.
     *
     * @param ps
     */    
    public void setPigContext(PigContext context);

    /**
     * Sets the id of the plan which should be globally unique.
     *
     * @param planId
     */
    void setPlanId(String planId);

    /**
     * Returns the id of the plan which should be globally unique.
     *
     * @return
     */
    String getPlanId();

    /**
     * Signals that progress has been made in at least one job.
     * Implementations should use this as a trigger to update state about the job(s).
     *
     * @param progress
     */
    void updateProgress(int progress);

    /**
     * Signals that a job has started.
     *
     * @param jobId
     */
    void jobStarted(String jobId);

    /**
     * Signals that a job has finished and provides stats about the job.
     * Implementations should use this as a trigger to update state about the job.
     *
     * @param jobStats
     */
    void jobFinished(JobStats jobStats);

    /**
     * Signals that a job has failed and provides stats about the job.
     * Implementations should use this as a trigger to update state about the job
     * as well as to update failure of the overall plan.
     *
     * @param jobStats
     */
    void jobFailed(JobStats jobStats);

    /**
     * Signals that a plan has completed.
     * Implementations should update the state of the plan accordingly.
     */
    void planCompleted();
}
