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

import java.util.Map;
import java.util.List;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import org.apache.pig.tools.pigstats.JobStats;
import com.netflix.lipstick.model.P2jWarning;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class JobWarnings {

    private static final Log log = LogFactory.getLog(JobWarnings.class);

    public final String NO_OUTPUT_RECORDS_KEY = "noOutputRecords";

    public static final String NO_OUTPUT_RECORDS_KEY = "noOutputRecords";
    public static final String SKEWED_REDUCERS_KEY = "skewedReducers";

    /* Require that there are a minimum num of reducer tasks to consider
       a job reducer skewed to prevent a high level of false posititives
       that ensue otherwise. */
    public static final int MIN_REDUCERS_FOR_SKEW = 2;

    /* We use the standard deviation as a reference for determining if
       a slow running reducer is skewing.  In order to deal with a set
       of reducers that has a small deviation, we override stddev in 
       the case that it is exceedingly small. This compensates for 
       reducers whose runtime is larger than the rest of the reducers
       in the job but whose runtime is still fast enough that we don't
       need to warn about it. */
    public static final double MIN_STDDEV_DELTA_MINUTES = 10;

    /* The size on disk of an HDFS directory. When a m/r job writes zero
       records it creates a directory for its output on HDFS. In order to
       tell the difference between records written and a directory written
       but no records written we compare the size of the output to this 
       contstant. */
    public static final long HDFS_DIRECTORY_SIZE = 154;

    public boolean shouldNoOutputRecordsWarn(JobStats jobStats, String jobId) {
        if (0 == jobStats.getRecordWrittern()) {
            log.info("JobStats reports no records have been written");
            /* JobStats is periodically returning zero for the number of records that
               have been written for map/reduce jobs where records *have* been written.
               Tracking down why/how this is happening has proved difficult, so to 
               prevent false positives we're double checking against a few well known
               counters to confirm that we don't have any record data being written out. */
            if (countersShowRecordsWritten(jobStats, jobId)) {
                log.info("Counters found records written, no warning should be sent");
                return false;
            } else {
                log.info("Counters also report no records written, will warn user");
                return true;
            }
        } else {
            log.info("JobStats reports some records have been written");
            return false;
        }
    }

    public long numOutputRecordsFromCounters(JobStats jobStats, String jobId) {
        JobClient jobClient = PigStats.get().getJobClient();
        Counters counters;
        try {
            RunningJob rj = jobClient.getJob(jobId);
            counters = rj.getCounters();
        } catch (IOException e) {
            log.error("Error getting job client, continuing", e);
            return 1;
        }

        Group fsGroup = counters.getGroup("FileSystemCounters");
        long hdfsBytes = fsGroup.getCounter("HDFS_BYTES_WRITTEN");
        long s3Bytes = fsGroup.getCounter("S3N_BYTES_WRITTEN");
        return hdfsBytes + s3Bytes;
    }

    public static class ReducerDuration {
        public String reducerTaskId;
        public double duration;
        public ReducerDuration(String reducerTaskId, double duration) {
            this.reducerTaskId = reducerTaskId;
            this.duration = duration;
        }
    }

    /* Version 0.0 attempt, if any of the top 10% of reducers,
       in terms of duration, are more than 2x the stddev from
       the mean of the bottom 90% we consider skew to be present.
       Version 0.1 pending more data about reducer skew.  This is
       to be considered a best effort for now. */
    public List<String> findSkewedReducers(List<ReducerDuration> reducerTimes) {
        if (! (MIN_REDUCERS_FOR_SKEW < reducerTimes.size())) {
            return Lists.newLinkedList();
        }

        int numPotentialOutliers = (int)Math.ceil(reducerTimes.size() / 10.0);
        int inflection = reducerTimes.size() - numPotentialOutliers;
        List<ReducerDuration> potentialOutliers = reducerTimes.subList(inflection, reducerTimes.size());
        List<ReducerDuration> referenceReducers = reducerTimes.subList(0, inflection);

        /* List of reducer duration values that we will compare the
           potential outliers to. */
        double[] referenceDurations = new double[referenceReducers.size()];
        for (int i = 0; i < referenceReducers.size(); i++) {
            referenceDurations[i] = referenceReducers.get(i).duration;
        }

        double refMean = StatUtils.mean(referenceDurations);
        double refVariance = StatUtils.populationVariance(referenceDurations, refMean);
        double refStdDev = Math.sqrt(refVariance);

        /* If the time to complete the task is more than this far
           from the mean of all task completion times, we consider
           it skewed */
        double distToMeanThreshold  = Math.max((refStdDev * 2), (MIN_STDDEV_DELTA_MINUTES * 60)) + refMean;

        /* Now collect and return any of the outliers whose distance
           from the mean is great than the computed threshold. */
        List<String> skewedReducerIds = Lists.newArrayList();
        for (ReducerDuration r: potentialOutliers) {
            if ((r.duration - refMean) > distToMeanThreshold) {
                skewedReducerIds.add(r.reducerTaskId);
            }
        }
        return skewedReducerIds;
    }

    public List<ReducerDuration> enumerateReducerRunTimesAccending(JobClient jobClient, String jobId) {
        try {
            TaskReport[] reduceTasks = jobClient.getReduceTaskReports(jobId);
            return enumerateReducerRunTimesAccending(reduceTasks);
        } catch (IOException e) {
            log.error("Error getting reduce task reports, continuing", e);
            return Lists.newArrayList();
        }
    }

    /* Extract all running or completed reducer tasks for the job, their runtime and sort them
       in accending order. Used to partition reduce tasks to detect reducer skew. */
    public List<ReducerDuration> enumerateReducerRunTimesAccending(TaskReport[] reduceTasks) {
        List<ReducerDuration> reducerDurations = Lists.newArrayList();
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < reduceTasks.length; i++) {
            String taskId = reduceTasks[i].getTaskID().toString();
            long startTime = reduceTasks[i].getStartTime();
            long finishTime = reduceTasks[i].getFinishTime();
            if (0 == finishTime) {
                /* Job hasn't finished yet */
                finishTime = now;
            }
            if (0 != startTime) {
                reducerDurations.add(new ReducerDuration(taskId, (double)finishTime - startTime));
            }
        }
        return reducerDurations;
    }

    public boolean countersShowRecordsWritten(JobStats jobStats, String jobId) {
        JobClient jobClient = PigStats.get().getJobClient();
        Counters counters;
        try {
            RunningJob rj = jobClient.getJob(jobId);
            counters = rj.getCounters();
        } catch (IOException e) {
            log.error("Error getting job client, continuing", e);
            return true;
        }

        Group fsGroup = counters.getGroup("FileSystemCounters");
        long hdfsBytes = fsGroup.getCounter("HDFS_BYTES_WRITTEN");
        long s3Bytes = fsGroup.getCounter("S3N_BYTES_WRITTEN");
        log.info(String.format("Total of %s bytes were written by this m/r job", (hdfsBytes + s3Bytes)));
        if ((0 == s3Bytes) && (HDFS_DIRECTORY_SIZE == hdfsBytes)) {
            log.info("No s3 output and empty HDFS directory created");
            return false;
        } else {
            return (0 < (hdfsBytes + s3Bytes));
        }
    }

    protected void addWarning(String jobId, Map<String, P2jWarning> warningsMap, String warningKey) {
        Map<String, String> attrs = Maps.newHashMap();
        addWarning(jobId, warningsMap, warningKey, attrs);
    }

    protected void addWarning(JobStats jobStats, Map<String, P2jWarning> warningsMap, String warningKey) {
        Map<String, String> attrs = Maps.newHashMap();
        addWarning(jobStats.getJobId(), warningsMap, warningKey, attrs);
    }

    protected void addWarning(String jobId, Map<String, P2jWarning> warningsMap, String warningKey, Map<String, String> attributes) {
        P2jWarning pw = new P2jWarning();
        pw.setWarningKey(warningKey);
        pw.setJobId(jobId);
        pw.setWarningAttributes(attributes);
        warningsMap.put(warningKey, pw);
    }

    public Map<String, P2jWarning> findCompletedJobWarnings(JobClient jobClient, JobStats jobStats) {
        Map<String, P2jWarning> warnings = findRunningJobWarnings(jobClient, jobStats.getJobId());
        if (shouldNoOutputRecordsWarn(jobStats, jobStats.getJobId())) {
            addWarning(jobStats, warnings, NO_OUTPUT_RECORDS_KEY);
        }
        return warnings;
    }

    public Map<String, P2jWarning> findRunningJobWarnings(JobClient jobClient, String jobId) {
        Map<String, P2jWarning> warnings = Maps.newHashMap();
        List<ReducerDuration> reducerTimes = enumerateReducerRunTimesAccending(jobClient, jobId);
        List<String> skewedReducerIds = findSkewedReducers(reducerTimes);
        if (0 < skewedReducerIds.size()) {
            /* todo: find a better why to shove a list into the attribute map
               than a csv.  I feel shame at this. */
            String sris = Joiner.on(",").join(skewedReducerIds);
                addWarning(jobId, warnings, SKEWED_REDUCERS_KEY, 
                           ImmutableMap.of(
                                           "skewedReducerIds", sris,
                                           "numberSkewedReducers", Integer.toString(skewedReducerIds.size())
                                           ));
        }
        return warnings;
    }
}
