/*
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
package pigstatus

import com.netflix.lipstick.model.P2jPlan;
import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlanStatus;
import com.netflix.lipstick.model.P2jSampleOutput;
import com.netflix.lipstick.model.P2jSampleOutputList;
import com.netflix.lipstick.Pig2DotGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.criterion.CriteriaSpecification

class PlanService {
    def om = new ObjectMapper()

    def save(params, jsonString) {
        def ser = om.readValue(jsonString, P2jPlanPackage.class)
        def uuid = ser.uuid
        def svg = new Pig2DotGenerator(ser.optimized).generatePlan("svg")
        ser.optimized.setSvg(svg)
        svg = new Pig2DotGenerator(ser.unoptimized).generatePlan("svg")
        ser.unoptimized.setSvg(svg)
        if (ser.save(flush:true)) {
            return  [
                uuid: uuid,
                aliases: ser.optimized.plan.collect {it.value.alias},
            ]
        } else {
            return  [
                error: "failed to save data"
            ]
        }
    }
	
	def addSampleOutput(id, jobId, jsonString)  {
		P2jSampleOutputList sampleOutputList = om.readValue(jsonString, P2jSampleOutputList.class);
		P2jPlanPackage plan = P2jPlanPackage.findByUuid(id)
		plan.getSampleOutputMap().put(jobId, sampleOutputList);
		if (plan.save(flush:true)) {
            return [
                uuid: id,
                jobId: jobId
            ]
        } else {
            return  [
                error: "failed to save sampleoutput"
            ]
        }
	}

    def list(params) {
        params.max = Math.min(params.max ? params.int('max') : 10, 100)
        params.sort = params.sort ?: "id"
        params.order = params.order ?: "desc"

        //TODO: this is because we pass these names back with the incorrect names
        switch (params.sort) {
            case "startTime":
            case "heartbeatTime":
                params.sort = "status." + params.sort
                break
            case "user":
                params.sort = "userName"
                break
        }
        def jobs = P2jPlanPackage.createCriteria().list(params) {
            if (params.status) {
                status {
                    eq("statusText", com.netflix.lipstick.model.P2jPlanStatus.StatusText.valueOf(params.status))
                }
            }
            if (params.search) {
                or {
                    ilike("userName", "%" + params.search + "%")
                    ilike("jobName", "%" + params.search + "%")
                }
            }
            join 'status'
            resultTransformer(CriteriaSpecification.ALIAS_TO_ENTITY_MAP)
            projections {
                property('id', 'id')
                property('uuid', 'uuid')
                property('userName', 'userName')
                property('jobName', 'jobName')
                status {
                    property('progress', 'progress')
                    property('statusText', 'status')
                    property('heartbeatTime', 'heartbeatTime')
                    property('startTime', 'startTime')
                }
            }
        }

        return [
            jobs: jobs,
            jobsTotal: jobs.getTotalCount()
        ]
    }

    def update(params, jsonString) {
        def plan = P2jPlanPackage.findByUuid(params.id)
        if (!plan) {
            return null
        }
        plan.status.updateWith(om.readValue(jsonString, P2jPlanStatus.class));
        return [ status: "updated uuid " + params.id ]
    }

    def delete(id) {
    }

    def get(params) {
        //This is a little complicated in that we have to add properties if they
        //are added to the root class.  We don't intend for that to happen often
        //we will be alright.  This is mainly for efficiency of querying and loading
        //only the data the client wishes to display.
        if (!params.status && !params.scripts && !params.optimized && !params.unoptimized
               && !params.sampleOutput) {
            params.full = true
        }

        def plan 
        if (!params.full) {
            plan = P2jPlanPackage.createCriteria().list() {
                eq('uuid', params.id)
                resultTransformer(CriteriaSpecification.ALIAS_TO_ENTITY_MAP)
                projections {
                    property('id', 'id')
                    property('uuid', 'uuid')
                    property('userName', 'userName')
                    property('jobName', 'jobName')
                    if (params.status) {
                        property('status', 'status')
                    }
                    if (params.scripts) {
                        property('scripts', 'scripts')
                    }
                    if (params.optimized) {
                        property('optimized', 'optimized')
                    }
                    if (params.unoptimized) {
                        property('unoptimized', 'unoptimized')
                    }
                }
            }
            if (!plan) {
                return null
            } else {
                plan = plan[0]
            }
            if (params.sampleOutput) {
                def temp = P2jPlanPackage.findByUuid(params.id)
                plan.sampleOutputMap = temp.sampleOutputMap
            }
        } else {
            plan = P2jPlanPackage.findByUuid(params.id)
        }
        return om.writeValueAsString(plan)
    }
}
