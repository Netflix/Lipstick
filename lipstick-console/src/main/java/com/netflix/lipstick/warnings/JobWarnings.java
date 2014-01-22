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
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;
import org.apache.pig.tools.pigstats.JobStats;
import com.netflix.lipstick.model.P2jWarning;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;


public class JobWarnings {

    private static final Log log = LogFactory.getLog(JobWarnings.class);
    public final String NO_OUTPUT_RECORDS_KEY = "noOutputRecords";

    public boolean shouldNoOuputRecordsWarn(JobStats jobStats, String jobId) {
        if (0 == jobStats.getRecordWrittern()) {
            log.info("JobStats reports no records have been written");
            /* JobStats is periodically returning zero for the number of records that
               have been written for map/reduce jobs where records *have* been written.
               Tracking down why/how this is happening has proved difficult, so to 
               prevent false positives we're double checking against a few well known
               counters to confirm that we don't have any record data being written out. */
            if (0 == numOutputRecordsFromCounters(jobStats, jobId)) {
                log.info("Counters also report no records written, will warn user");
                return true;
            } else {
                log.info("Counters found records written, no warning should be sent");
                return false;
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

    protected void addWarning(JobStats jobStats, Map<String, P2jWarning> warningsMap, String warningKey) {
        Map<String, String> attrs = Maps.newHashMap();
        addWarning(jobStats, warningsMap, warningKey, attrs);
    }

    protected void addWarning(JobStats jobStats, Map<String, P2jWarning> warningsMap, String warningKey, Map<String, String> attributes) {
        P2jWarning pw = new P2jWarning();
        pw.setWarningKey(warningKey);
        pw.setJobId(jobStats.getJobId());
        pw.setWarningAttributes(attributes);
        warningsMap.put(warningKey, pw);
    }

    public Map<String, P2jWarning> findCompletedJobWarnings(JobStats jobStats) {
        Map<String, P2jWarning> warnings = Maps.newHashMap();
        if (shouldNoOuputRecordsWarn(jobStats, jobStats.getJobId())) {
            addWarning(jobStats, warnings, NO_OUTPUT_RECORDS_KEY);
        }
        return warnings;
    }
}
