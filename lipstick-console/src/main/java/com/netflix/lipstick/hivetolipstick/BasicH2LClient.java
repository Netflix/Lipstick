package com.netflix.lipstick.hivetolipstick;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.TaskType;
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

    protected static final String USER_NAME_PROP = "user.name";
    protected static final String JOB_NAME_PROP = "jobName";

    protected PigStatusClient psClient;
    protected JobClient jobClient;

    protected boolean planFailed = false;
    protected String planId;
    protected QueryPlan queryPlan;

    private static Object instanceLock = new Object();
    private static BasicH2LClient instance;

    private static Set<String> completedStages = Sets.newHashSet();
    private static Set<String> readyOrRunningStages = Sets.newHashSet();
    private static Map<String, Task<? extends Serializable>> stageIdToExecTaskMap = Maps.newHashMap();

    public UUID id = UUID.randomUUID();

    /**
     * Instantiates a new BasicH2LClient using RestfulPigStatusClient with
     * serviceUrl.
     *
     * @param serviceUrl
     *            the url to connect to the Lipstick Server
     */
    private BasicH2LClient(String serviceUrl) {
        psClient = new RestfulPigStatusClient(serviceUrl);
        try {
            jobClient = new JobClient(new JobConf(SessionState.get().getConf()));
        } catch (IOException e) {
            e.printStackTrace();
            jobClient = null;
        }
    }

    public static BasicH2LClient getInstance(String serviceUrl) {
        synchronized(instanceLock) {
            if(instance == null) {
                instance = new BasicH2LClient(serviceUrl);
            }
            return instance;
        }
    }

    @Override
    public String getPlanId() {
        return planId;
    }

    @Override
    public synchronized void preExecute(HookContext context) {
        if (context != null && context.getQueryPlan() != null && this.queryPlan == null) {
            try {
                queryPlan = context.getQueryPlan();
                planId = queryPlan.getQueryId();

                P2jPlanPackage plans = new P2jPlanPackage(new HivePlanGenerator(queryPlan).getP2jPlan(),
                                                          new HivePlanGenerator(queryPlan).getP2jPlan(),
                                                          queryPlan.getQueryString(),
                                                          planId);

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

                Thread monitorThread = new Thread() {
                    @Override
                    public void run() {
                        monitor();
                    }
                };

                monitorThread.start();
            } catch (Exception e) {
                LOG.warn("Caught unexpected exception generating json plan.", e);
            }
        }
    }

    @Override
    public void postExecute() {
        try {
            boolean shouldSave = false;
            P2jPlanStatus planStatus = new P2jPlanStatus();

            for (Stage s : queryPlan.getQueryPlan().getStageList()) {
                shouldSave ^= updatePlanStatus(s, planStatus);
            }

            if(shouldSave) {
                psClient.saveStatus(planId, planStatus);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void run(Map<String, Double> counters, String jobId) {
        //NoOp for now
    }

    private void monitor() {
        while (true) {
            try {
                P2jPlanStatus planStatus = new P2jPlanStatus();
                boolean saveStatus = false;

                List<Stage> stageList = queryPlan.getQueryPlan().getStageList();
                List<Adjacency> aList = queryPlan.getQueryPlan().getStageGraph().getAdjacencyList();
                Set<String> currentStages = getCurrentStageNames(stageList, aList, Sets.newHashSet(completedStages));

                for (Stage s : stageList) {
                    if (currentStages.contains(s.getStageId())) {
                        saveStatus ^= updatePlanStatus(s, planStatus);
                    }
                }

                if (saveStatus) {
                    psClient.saveStatus(planId, planStatus);
                }
            } catch (Exception e) {
                LOG.info("Caught unexpected exception during monitor.", e);
            }
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }
        }
    }

    private Set<String> getCurrentStageNames(List<Stage> fullStageList, List<Adjacency> aList, Set<String> completedStageNames) {
        Set<String> roots = Sets.newHashSet();
        Map<String, List<String>> nodeToChildrenMap = Maps.newHashMap();
        //Add all stage ids to roots
        for(Stage s : fullStageList) {
            roots.add(s.getStageId());
        }

        //Remove all stage ids that are children from the roots set
        //Populate the node to children map
        if(aList != null) {
            for(Adjacency adj : aList) {
                String nodeName = adj.getNode();
                nodeToChildrenMap.put(nodeName, new LinkedList<String>());
                for(String child : adj.getChildren()) {
                    roots.remove(child);
                    nodeToChildrenMap.get(nodeName).add(child);
                }
            }
        }

        //Replace completed stages w/ their children
        //Continue until we've exhausted the completed stages
        while(completedStageNames.size() > 0) {
            List<String> completedNamesToRemove = Lists.newLinkedList();
            for(String completedStage : completedStageNames) {
                if(roots.contains(completedStage)) {
                    roots.remove(completedStage);

                    if(nodeToChildrenMap.get(completedStage) != null) {
                        for(String child : nodeToChildrenMap.get(completedStage)) {
                            roots.add(child);
                        }
                    }

                    completedNamesToRemove.add(completedStage);
                }
            }
            for(String toRemove : completedNamesToRemove) {
                completedStageNames.remove(toRemove);
            }
        }

        return roots;
    }

    private boolean updatePlanStatus(Stage s, P2jPlanStatus planStatus) {
        try {
            String stageId = s.getStageId();
            P2jJobStatus jobStatus = null;
            String formattedJobName = "scope-" + stageId.replaceAll("-", "");
            if(!completedStages.contains(stageId) && stageIdToExecTaskMap.get(stageId).getJobID() != null) {
                jobStatus = buildCompletedJobStatusMap(stageIdToExecTaskMap.get(stageId).getJobID(), formattedJobName);
            } else if (!readyOrRunningStages.contains(stageId) && !completedStages.contains(stageId)) {
                jobStatus = buildRunningJobStatusMap(s, formattedJobName);
                readyOrRunningStages.add(stageId);
            } else {
                return false;
            }

            if (s.isDone() && jobStatus != null) {
                readyOrRunningStages.remove(stageId);
                completedStages.add(stageId);

                if(!jobStatus.getIsSuccessful()  && !planFailed) {
                    planFailed = true;
                    planStatus.setStatusText(StatusText.failed);
                }

                //Get Sample Output if plan hasn't failed
                if(!planFailed) {
                    P2jSampleOutputList sampleList = getSampleOutput(stageId);
                    psClient.saveSampleOutput(planId, formattedJobName, sampleList);
                }

                planStatus.setProgress((int)(100f * ((float) completedStages.size() / (float) queryPlan.getQueryPlan().getStageList().size())));

                if(completedStages.size() == queryPlan.getQueryPlan().getStageList().size() && !planFailed) {
                    planStatus.setStatusText(StatusText.finished);
                    planStatus.setProgress(100);
                    planStatus.setEndTime();
                }
            }
            if(jobStatus != null) {
                planStatus.updateWith(jobStatus);
                return true;
            }
        } catch(Exception e) {
            LOG.info("Caught unexpected exception during updatePlanStatus.", e);
        }
        return false;
    }

    protected P2jSampleOutputList getSampleOutput(String stageId) {
        P2jSampleOutputList sampleOutputList = new P2jSampleOutputList();

        for (Task<? extends Serializable> task : getAllTasks(queryPlan.getRootTasks(), null)) {
            if(task.getId().equals(stageId)) {
                try {
                    for(Operator<? extends Serializable> op : getAllOperators(task)) {
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
            }
        }

        return sampleOutputList;
    }

    protected Map<String, Task<? extends Serializable>> buildStageIdToExecTaskMap(QueryPlan queryPlan) {
        Map<String, Task<? extends Serializable>> stageIdToExecTaskMap = Maps.newHashMap();
        try {
            Set<String> stageIds = Sets.newHashSet();
            for(Stage s : queryPlan.getQueryPlan().getStageList()) {
                stageIds.add(s.getStageId());
            }

            for(Task<? extends Serializable> task : getAllTasks(queryPlan.getRootTasks(), null)) {
                if(stageIds.contains(task.getId())) {
                    stageIdToExecTaskMap.put(task.getId(), task);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return stageIdToExecTaskMap;
    }

    protected List<Task<? extends Serializable>> getAllTasks(Collection<Task<? extends Serializable>> thisLevelTasks, List<Task<? extends Serializable>> allTasks) {
        if(allTasks == null) {
            allTasks = Lists.newArrayList();
        }

        for(Task<? extends Serializable> task : thisLevelTasks) {
            allTasks.add(task);

            if(task.getChildTasks() != null) {
                getAllTasks(task.getChildTasks(), allTasks);
            }
        }

        return allTasks;
    }

    protected List<Operator<? extends Serializable>> getAllOperators(Task<? extends Serializable> task) {
        List<Operator<? extends Serializable>> topOps = new ArrayList<Operator<? extends Serializable>>(task.getTopOperators());
        //The reducer task is not accessible through the 'getTopOperators()' tree, add it
        if(task.getReducer() != null) {
            topOps.add(task.getReducer());
        }
        return getAllOperators(topOps, null);
    }

    protected List<Operator<? extends Serializable>> getAllOperators(Collection<Operator<? extends Serializable>> thisLevelOps, List<Operator<? extends Serializable>> allOps) {
        if(allOps == null) {
            allOps = Lists.newArrayList();
        }

        for(Operator<? extends Serializable> op : thisLevelOps) {
            allOps.add(op);
            if(op.getChildOperators() != null) {
                getAllOperators(op.getChildOperators(), allOps);
            }
        }

        return allOps;
    }

    protected P2jJobStatus buildRunningJobStatusMap(Stage s, String formattedJobName) {
        P2jJobStatus js = new P2jJobStatus();

        try {
            for(org.apache.hadoop.hive.ql.plan.api.Task t : s.getTaskList()) {
                float taskProgress = getTaskProgress(t);
                if(t.getTaskType().equals(TaskType.MAP)) {
                    js.setMapProgress(taskProgress);
                } else {
                    js.setReduceProgress(taskProgress);
                }
            }

            js.setJobId(formattedJobName);
            js.setJobName(formattedJobName);
            js.setScope(formattedJobName);
            js.setIsComplete(false);

            return js;
        } catch (Exception e) {
            LOG.error("Error getting job info.", e);
        }

        return null;
    }

    /**
     * Build a P2jJobStatus object for the map/reduce job with id jobId.
     *
     * @param jobId the id of the map/reduce job
     * @return the newly created P2jJobStatus
     */
    @SuppressWarnings("deprecation")
    protected P2jJobStatus buildCompletedJobStatusMap(String jobId, String formattedJobName) {
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
            js.setJobId(formattedJobName);
            js.setJobName(formattedJobName);
            js.setScope(formattedJobName);
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

    private float getTaskProgress(org.apache.hadoop.hive.ql.plan.api.Task t) {
        float completedOps = 0.0f;
        for(org.apache.hadoop.hive.ql.plan.api.Operator op : t.getOperatorList()) {
            if(op.isDone()) {
                completedOps += 1.0f;
            }
        }

        return completedOps / (t.getOperatorListSize());
    }
}
