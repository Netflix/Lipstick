package com.netflix.lipstick.hivetolipstick;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.ClientStatsPublisher;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TaskReport;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.lipstick.HivePlanGenerator;
import com.netflix.lipstick.model.P2jCounters;
import com.netflix.lipstick.model.P2jJobStatus;
import com.netflix.lipstick.model.P2jPlanPackage;
import com.netflix.lipstick.model.P2jPlanStatus;
import com.netflix.lipstick.model.P2jSampleOutput;
import com.netflix.lipstick.model.P2jPlanStatus.StatusText;
import com.netflix.lipstick.model.P2jSampleOutputList;
import com.netflix.lipstick.pigstatus.PigStatusClient;
import com.netflix.lipstick.pigstatus.RestfulPigStatusClient;
import com.netflix.lipstick.util.HiveOutputSampler;
import com.netflix.lipstick.util.OutputSampler.SampleOutput;

public class BasicH2LClient implements H2LClient, ClientStatsPublisher {

    private static final Log LOG = LogFactory.getLog(H2LClient.class);

    protected static final Pattern STAGEID_PATTERN = Pattern.compile("^.*\\((Stage\\-\\d+)\\)$", Pattern.DOTALL);

    protected static final String USER_NAME_PROP = "user.name";
    protected static final String JOB_NAME_PROP = "jobName";
    protected static final String LIPSTICK_URL_PROP = "lipstick.server.url";

    protected static PigStatusClient psClient;
    protected static JobClient jobClient;

    protected static volatile boolean planFailed = false;
    protected static String planId;
    protected static QueryPlan queryPlan;

    private static Object instanceLock = new Object();
    private static BasicH2LClient instance;

    private static Set<String> completedJobIds = Sets.newHashSet();
    private static Map<String, P2jSampleOutputList> successfulJobIdsToSampleOutput = Maps.newHashMap();
    private static Set<String> runningJobIds = Sets.newHashSet();
    private static Map<String, String> jobIdToStageIdMap = Maps.newConcurrentMap();
    private static Map<String, Task<? extends Serializable>> stageIdToExecTaskMap = Maps.newHashMap();

    //Will be called in Hive11 or greater as a listener.
    //From empirical evidence, this seems to always be called after 'preExecute'
    public BasicH2LClient() {
        synchronized(instanceLock) {
            if(instance == null) {
                try {
                    String serviceUrl = SessionState.get().getConf().getAllProperties().getProperty(LIPSTICK_URL_PROP);
                    psClient = new RestfulPigStatusClient(serviceUrl);
                    jobClient = new JobClient(new JobConf(SessionState.get().getConf()));
                } catch (IOException e) {
                    LOG.error("Caught unexpected exception creating BasicH2LClient.", e);
                }
                instance = this;
            }
        }
    }

    public static BasicH2LClient getInstance() {
        synchronized(instanceLock) {
            if(instance == null) {
                instance = new BasicH2LClient();
            }
            return instance;
        }
    }

    @Override
    public synchronized void preExecute(HookContext context) {
        if (context != null && context.getQueryPlan() != null) {
            try {
                queryPlan = context.getQueryPlan();
                planId = queryPlan.getQueryId();

                P2jPlanPackage plans = new P2jPlanPackage(new HivePlanGenerator(queryPlan).getP2jPlan(), new HivePlanGenerator(queryPlan).getP2jPlan(),
                                                          queryPlan.getQueryString(), planId);

                stageIdToExecTaskMap = buildStageIdToExecTaskMap(queryPlan);

                Properties props = context.getConf().getAllProperties();
                if (props.containsKey(USER_NAME_PROP)) {
                    plans.setUserName(props.getProperty(USER_NAME_PROP));
                } else {
                    plans.setUserName(System.getProperty(USER_NAME_PROP));
                }
                if (props.containsKey(JOB_NAME_PROP)) {
                    plans.setJobName(props.getProperty(JOB_NAME_PROP));
                } else {
                    plans.setJobName("unknown");
                }
                plans.getStatus().setStartTime();
                plans.getStatus().setStatusText(StatusText.running);
                psClient.savePlan(plans);
            } catch (Exception e) {
                LOG.warn("Caught unexpected exception generating json plan.", e);
            }
        }
    }

    @Override
    public void run(Map<String, Double> counters, String jobId) {
        if(jobClient != null) {
            if(!runningJobIds.contains(jobId)) {
                runningJobIds.add(jobId);
                jobIdToStageIdMap.put(jobId, getStageIdFromJobId(jobId));
            }

            P2jPlanStatus planStatus = new P2jPlanStatus();
            List<String> newlyCompletedJobs = Lists.newLinkedList();
            float partialCompletionSum = 0.0f;
            for(String runningJobId : runningJobIds.toArray(new String[0])) {
                P2jJobStatus jobStatus = buildJobStatus(runningJobId);
                planStatus.updateWith(jobStatus);

                if(jobStatus.getIsComplete()) {
                    if(jobStatus.getIsSuccessful()) {
                        successfulJobIdsToSampleOutput.put(runningJobId, new P2jSampleOutputList());
                    } else {
                        planFailed = true;
                        planStatus.setStatusText(StatusText.failed);
                    }
                    newlyCompletedJobs.add(runningJobId);
                } else {
                    partialCompletionSum += 100.0f * jobStatus.getMapProgress() + 100.0f * jobStatus.getReduceProgress();
                }
            }

            for(String newlyCompletedJob : newlyCompletedJobs) {
                runningJobIds.remove(newlyCompletedJob);
                completedJobIds.add(newlyCompletedJob);
            }

            planStatus.setProgress(calculatePlanProgress(partialCompletionSum));
            psClient.saveStatus(planId, planStatus);
            getSampleOutputs();
        }
    }

    @Override
    public void postExecute() {
        P2jPlanStatus planStatus = new P2jPlanStatus();

        if(!planFailed) {
            planStatus.setStatusText(StatusText.finished);
            planStatus.setProgress(100);
        }

        planStatus.setEndTime();
        psClient.saveStatus(planId, planStatus);
        getSampleOutputs();
    }

    protected int calculatePlanProgress(float partialProgress) {
        float totalPossibleProgress = stageIdToExecTaskMap.size() * 200.0f;
        float completedProgress = completedJobIds.size() * 200.0f;
        float currentProgress = completedProgress + partialProgress;

        return (int)(100.0f * currentProgress/totalPossibleProgress);
    }

    /**
     * Method should continue to get called during progress updates b/c output is not always available immediately after a success.
     */
    protected void getSampleOutputs() {
        for(String jobId : successfulJobIdsToSampleOutput.keySet()) {
            P2jSampleOutputList currSample = successfulJobIdsToSampleOutput.get(jobId);
            if(currSample.getSampleOutputList() == null || currSample.getSampleOutputList().size() == 0) {
                String stageId = jobIdToStageIdMap.get(jobId);
                P2jSampleOutputList output = getSampleOutput(stageId);
                if(output != null && output.getSampleOutputList() != null && output.getSampleOutputList().size() > 0) {
                    currSample = output;
                    psClient.saveSampleOutput(planId, formatStageId(stageId), output);
                }
            }
        }
    }

    protected P2jSampleOutputList getSampleOutput(String stageId) {
        P2jSampleOutputList sampleOutputList = new P2jSampleOutputList();

        try {
            Task<? extends Serializable> task = stageIdToExecTaskMap.get(stageId);
            for(Operator<? extends OperatorDesc> op : getAllOperators(task)) {
                //Produce sampleOutput for each FileSinkOperator
                if(op instanceof FileSinkOperator) {
                    FileSinkOperator fsop = (FileSinkOperator)op;

                    if(fsop.getConf().getTableInfo().getInputFileFormatClassName().equals(SequenceFileInputFormat.class.getCanonicalName())) {
                        HiveOutputSampler hos = new HiveOutputSampler(fsop);
                        for(SampleOutput sample : hos.getSampleOutputs(10, 1024)) {
                            P2jSampleOutput p2jSampleOutput = new P2jSampleOutput();
                            p2jSampleOutput.setSchemaString(sample.getSchema());
                            p2jSampleOutput.setSampleOutput(sample.getOutput());
                            sampleOutputList.add(p2jSampleOutput);
                        }
                    }
                }
            }
        } catch(Exception e) {
            LOG.info("Caught unexpected exception trying to get sample output for stage [" + stageId + "].", e);
        }

        return sampleOutputList;
    }

    protected Map<String, Task<? extends Serializable>> buildStageIdToExecTaskMap(QueryPlan queryPlan) {
        Map<String, Task<? extends Serializable>> stageIdToTaskMap = getFullTaskMap(queryPlan.getRootTasks(), null);

        List<String> nonMapRedStages = Lists.newLinkedList();
        for(String stageId : stageIdToTaskMap.keySet()) {
            Task<? extends Serializable> task = stageIdToTaskMap.get(stageId);
            if(task.getType() != StageType.MAPRED) {
                nonMapRedStages.add(stageId);
            }
        }

        for(String nonMapRedStage : nonMapRedStages) {
            stageIdToTaskMap.remove(nonMapRedStage);
        }

        return stageIdToTaskMap;
    }

    protected Map<String, Task<? extends Serializable>> getFullTaskMap(Collection<Task<? extends Serializable>> thisLevelTasks, Map<String, Task<? extends Serializable>> stageIdToTaskMap) {
        if(stageIdToTaskMap == null) {
            stageIdToTaskMap = Maps.newHashMap();
        }

        for(Task<? extends Serializable> task : thisLevelTasks) {
            if(!stageIdToTaskMap.containsKey(task.getId())) {
                stageIdToTaskMap.put(task.getId(), task);

                if(task.getChildTasks() != null) {
                    getFullTaskMap(task.getChildTasks(), stageIdToTaskMap);
                }

                if (task.getDependentTasks() != null) {
                    getFullTaskMap(task.getDependentTasks(), stageIdToTaskMap);
                }
            }
        }

        return stageIdToTaskMap;
    }

    protected List<Operator<? extends OperatorDesc>> getAllOperators(Task<? extends Serializable> task) {
        if(task == null) {
            return Lists.newArrayList();
        }

        List<Operator<? extends OperatorDesc>> topOps = new ArrayList<Operator<? extends OperatorDesc>>(task.getTopOperators());
        //The reducer task is not accessible through the 'getTopOperators()' tree, add it
        if(task.getReducer() != null) {
            topOps.add(task.getReducer());
        }
        return getAllOperators(topOps, null);
    }

    protected List<Operator<? extends OperatorDesc>> getAllOperators(Collection<Operator<? extends OperatorDesc>> thisLevelOps, List<Operator<? extends OperatorDesc>> allOps) {
        if(allOps == null) {
            allOps = Lists.newArrayList();
        }

        for(Operator<? extends OperatorDesc> op : thisLevelOps) {
            allOps.add(op);
            if(op.getChildOperators() != null) {
                getAllOperators(op.getChildOperators(), allOps);
            }
        }

        return allOps;
    }

    protected P2jJobStatus buildJobStatus(String jobId) {
        try {
            RunningJob rj = jobClient.getJob(jobId);
            if (rj == null) {
                LOG.warn("Couldn't find job status for jobId=" + jobId);
                return null;
            }

            String formattedJobName = formatStageId(jobIdToStageIdMap.get(jobId));
            P2jJobStatus js = new P2jJobStatus();

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
            js.setJobId(formattedJobName);
            js.setJobName(formattedJobName);
            js.setScope(formattedJobName);
            js.setTrackingUrl(rj.getTrackingURL());
            js.setIsComplete(rj.isComplete());
            if(rj.isComplete()) {
                js.setIsSuccessful(rj.isSuccessful());
            }
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

    protected String getStageIdFromJobId(String jobId) {
        RunningJob rj = null;
        try {
            rj = jobClient.getJob(jobId);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (rj == null) {
            LOG.warn("Couldn't find job status for jobId=" + jobId);
            return null;
        }

        Matcher matcher = STAGEID_PATTERN.matcher(rj.getJobName());
        if (matcher.find()) {
            String stageId = matcher.group(1);
            return stageId;
        } else {
            return null;
        }
    }

    protected String formatStageId(String stageId) {
        return "scope-" + stageId.replaceAll("-", "");
    }
}
