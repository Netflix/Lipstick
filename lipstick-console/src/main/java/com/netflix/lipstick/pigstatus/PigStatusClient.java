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
package com.netflix.lipstick.pigstatus;

import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlanStatus;
import com.netflix.lipstick.model.P2jSampleOutputList;

/**
 *
 * Interface for Lipstick client communication to server.
 *
 * @author nbates
 *
 */
public interface PigStatusClient {
    /**
     * Persists a P2JPlanPackage which will presumably be used
     * by the server with which the client is interacting.
     *
     * @param plans
     * @return
     */
    String savePlan(P2jPlanPackage plans);

    /**
     * Saves the status of the P2jPlanStatus.
     * It's expected that this will trigger an update of the
     * P2jPlanPackage with the given uuid.
     *
     * @param uuid
     * @param status
     */
    void saveStatus(String uuid, P2jPlanStatus status);

    /**
     * Saves the sample output for a given job.
     * It's expected that this will trigger an update of the
     * P2jPlanPackage with the given uuid.
     *
     * @param uuid
     * @param jobId
     * @param sampleOutputList
     */
    void saveSampleOutput(String uuid, String jobId, P2jSampleOutputList sampleOutputList);
}
