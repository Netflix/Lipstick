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
import com.google.common.collect.Maps;
import org.apache.pig.tools.pigstats.JobStats;
import com.netflix.lipstick.model.P2jWarning;

public class JobWarnings {

    public final String NO_OUTPUT_RECORDS_KEY = "noOutputRecords";

    public boolean shouldNoOuputRecordsWarn(JobStats jobStats) {
        if (0 == jobStats.getRecordWrittern()) {
            return true;
        } else {
            return false;
        }
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
        if (shouldNoOuputRecordsWarn(jobStats)) {
            addWarning(jobStats, warnings, NO_OUTPUT_RECORDS_KEY);
        }
        return warnings;
    }
}
