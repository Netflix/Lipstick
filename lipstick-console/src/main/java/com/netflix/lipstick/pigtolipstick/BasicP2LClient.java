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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.LipstickPigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.lipstick.MRPlanCalculator;
import com.netflix.lipstick.P2jPlanGenerator;
import com.netflix.lipstick.model.P2jCounters;
import com.netflix.lipstick.model.P2jJobStatus;
import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlanStatus;
import com.netflix.lipstick.model.P2jPlanStatus.StatusText;
import com.netflix.lipstick.model.P2jSampleOutput;
import com.netflix.lipstick.model.P2jSampleOutputList;
import com.netflix.lipstick.pigstatus.PigStatusClient;
import com.netflix.lipstick.pigstatus.RestfulPigStatusClient;
import com.netflix.lipstick.util.OutputSampler;
import com.netflix.lipstick.util.OutputSampler.SampleOutput;

/**
 * A basic implementation of P2LClient.
 *
 * @author nbates
 *
 */
public class BasicP2LClient implements P2LClient {
    private static final Log LOG = LogFactory.getLog(BasicP2LClient.class);

    protected static final String JOB_NAME_PROP = "jobName";

    protected boolean planFailed = false;
    protected String planId;
    protected P2jPlanGenerator unopPlanGenerator;
    protected P2jPlanGenerator opPlanGenerator;
    protected LipstickPigServer ps;
    protected final Set<String> runningJobIds = Sets.newHashSet();
    protected final Map<String, String> jobIdToScopeNameMap = Maps.newHashMap();

    protected final PigStatusClient psClient;

    /**
     * Instantiates a new BasicP2LClient using RestfulPigStatusClient with serviceUrl.
     *
     * @param serviceUrl the url to connect to the Lipstick Server
     */
    public BasicP2LClient(String serviceUrl) {
        this(new RestfulPigStatusClient(serviceUrl));
    }

    public BasicP2LClient(PigStatusClient client) {
        this.psClient = client;
    }

    @Override
    public void setPlanGenerators(P2jPlanGenerator unopPlanGenerator, P2jPlanGenerator opPlanGenerator) {
        this.unopPlanGenerator = unopPlanGenerator;
        this.opPlanGenerator = opPlanGenerator;
    }

    @Override
    public void setPigServer(LipstickPigServer ps) {
        this.ps = ps;
    }

    @Override
    public void setPlanId(String planId) {
        this.planId = planId;
    }

    @Override
    public String getPlanId() {
        return planId;
    }

    @Override
    @SuppressWarnings("unused")
    public void createPlan(MROperPlan plan) {
        if (plan != null && unopPlanGenerator != null && opPlanGenerator != null && ps != null) {
            Configuration conf = null;
            for (MapReduceOper job : plan) {
                if (conf == null) {
                    conf = new Configuration();
                    ScriptState.get().addSettingsToConf(job, conf);
                    break;
                }
            }
            try {
                Map<PhysicalOperator, Operator> p2lMap = Maps.newHashMap();
                Map<Operator, PhysicalOperator> l2pMap = ps.getPigContext().getExecutionEngine().getLogToPhyMap();
                for (Entry<Operator, PhysicalOperator> i : l2pMap.entrySet()) {
                    p2lMap.put(i.getValue(), i.getKey());
                }

                String script = null;

                // suppress getting script from conf for now - do something smarter later
                if (conf != null && false) {
                    script = new String(Base64.decodeBase64(conf.get("pig.script")));
                }
                if (script == null || script.length() == 0) {
                    script = StringUtils.join(ps.getScriptCache(), '\n');
                }

                MRPlanCalculator opPlan = new MRPlanCalculator(opPlanGenerator.getP2jPlan(), plan, p2lMap, opPlanGenerator.getReverseMap());
                MRPlanCalculator unopPlan = new MRPlanCalculator(unopPlanGenerator.getP2jPlan(), plan, p2lMap, unopPlanGenerator.getReverseMap());

                P2jPlanPackage plans = new P2jPlanPackage(opPlan.getP2jPlan(), unopPlan.getP2jPlan(), script, planId);

                Properties props = ps.getPigContext().getProperties();
                plans.setUserName(UserGroupInformation.getCurrentUser().getUserName());
                if (props.containsKey(JOB_NAME_PROP)) {
                    plans.setJobName(props.getProperty(JOB_NAME_PROP));
                } else {
                    plans.setJobName("unknown");
                }
                plans.getStatus().setStartTime();
                plans.getStatus().setStatusText(StatusText.running);
                psClient.savePlan(plans);
                

            } catch (Exception e) {
                LOG.error("Caught unexpected exception generating json plan.", e);
            }
        } else {
            LOG.warn("Not saving plan, missing necessary objects to do so");
        }
    }

    @Override
    public void updateProgress(int progress) {
        P2jPlanStatus planStatus = new P2jPlanStatus();
        planStatus.setProgress(progress);

        // toArray() done to avoid concurrent access errors during iteration
        for (String jobId : runningJobIds.toArray(new String[0])) {
            updatePlanStatusForJobId(planStatus, jobId);
        }

        psClient.saveStatus(planId, planStatus);
    }

    @Override
    public void jobStarted(String jobId) {
        PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
        LOG.debug("jobStartedNotification - jobId " + jobId + ", jobGraph:\n" + jobGraph);

        // for each job in the graph, check if the stats for a job with this
        // name is found. If so, look up it's scope and bind the jobId to
        // the DAGNode with the same scope.
        for (JobStats jobStats : jobGraph) {
            if (jobId.equals(jobStats.getJobId())) {
                LOG.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + jobId);
                jobIdToScopeNameMap.put(jobId, jobStats.getName());
                runningJobIds.add(jobId);
            }
        }
    }

    @Override
    public void jobFinished(JobStats jobStats) {
        // Remove jobId from runningSet b/c it's now complete
        String jobId = jobStats.getJobId();
        if (!runningJobIds.remove(jobId)) {
            LOG.error("Internal Error.  Job finished with no record of running jobId: " + jobId);
        }

        // Update the status of this job
        P2jPlanStatus planStatus = new P2jPlanStatus();
        updatePlanStatusForJobId(planStatus, jobId);
        psClient.saveStatus(planId, planStatus);

        // Get sample output for the job
        try {
            P2jSampleOutputList sampleOutputList = new P2jSampleOutputList();
            OutputSampler os = new OutputSampler(jobStats);
            // The 10 & 1024 params (maxRows and maxBytes)
            // should be configurable via properties
            for (SampleOutput schemaOutputPair : os.getSampleOutputs(10, 1024)) {
                P2jSampleOutput sampleOutput = new P2jSampleOutput();
                sampleOutput.setSchemaString(schemaOutputPair.getSchema());
                sampleOutput.setSampleOutput(schemaOutputPair.getOutput());
                sampleOutputList.add(sampleOutput);
            }
            psClient.saveSampleOutput(planId, jobIdToScopeNameMap.get(jobStats.getJobId()), sampleOutputList);
        } catch (Exception e) {
            LOG.error("Unable to get sample output from job with id [" + jobStats.getJobId() + "]. ", e);
        }
    }

    @Override
    public void jobFailed(JobStats jobStats) {
        planFailed = true;
    }

    @Override
    public void planCompleted() {
        if (planFailed) {
            planEndedWithStatusText(StatusText.failed);
        } else {
            planEndedWithStatusText(StatusText.finished);
        }
    }

    /**
     * Set the planStatus as ended with status statusText and saveStatus to the client.
     *
     * @param statusText job state at completition
     */
    protected void planEndedWithStatusText(StatusText statusText) {
        P2jPlanStatus planStatus = new P2jPlanStatus();
        planStatus.setEndTime();
        planStatus.setStatusText(statusText);
        psClient.saveStatus(planId, planStatus);
    }

    /**
     * Update planStatus with status for a map/reduce job.
     *
     * @param planStatus the P2jPlanStatus object to update
     * @param jobId the map/reduce job id
     */
    protected void updatePlanStatusForJobId(P2jPlanStatus planStatus, String jobId) {
        P2jJobStatus status = buildJobStatusMap(jobId);
        if (status != null) {
            status.setScope(jobIdToScopeNameMap.get(jobId));
            planStatus.updateWith(status);
        }
    }

    /**
     * Build a P2jJobStatus object for the map/reduce job with id jobId.
     *
     * @param jobId the id of the map/reduce job
     * @return the newly created P2jJobStatus
     */
    @SuppressWarnings("deprecation")
    protected P2jJobStatus buildJobStatusMap(String jobId) {
        JobClient jobClient = PigStats.get().getJobClient();
        P2jJobStatus js = new P2jJobStatus();

        try {
            RunningJob rj = jobClient.getJob(jobId);
            if (rj == null) {
                LOG.warn("Couldn't find job status for jobId=" + jobId);
                return null;
            }

            JobID jobID = rj.getID();
            Counters counters = rj.getCounters();
            Map<String, P2jCounters> cMap = Maps.newHashMap();
            for (Group g : counters) {
                P2jCounters countersObj = new P2jCounters();
                cMap.put(g.getDisplayName(), countersObj);
                for (Counter c : g) {
                    countersObj.getCounters().put(c.getDisplayName(), c.getValue());
                }
            }

            js.setCounters(cMap);
            TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobID);
            TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobID);
            js.setJobId(jobId.toString());
            js.setJobName(rj.getJobName());
            js.setTrackingUrl(rj.getTrackingURL());
            js.setIsComplete(rj.isComplete());
            js.setIsSuccessful(rj.isSuccessful());
            js.setMapProgress(rj.mapProgress());
            js.setReduceProgress(rj.reduceProgress());
            js.setTotalMappers(mapTaskReport.length);
            js.setTotalReducers(reduceTaskReport.length);
            return js;
        } catch (IOException e) {
            LOG.error("Error getting job info.", e);
        }

        return null;
    }
}
