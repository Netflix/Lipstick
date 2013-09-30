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
package com.netflix.lipstick.util;

import java.lang.Class;
import java.lang.reflect.Field;
import java.lang.IllegalAccessException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.io.TFileStorage;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.ScriptState;

/**
 * Output sampler of intermediate results of Pig jobs.
 *
 * @author nbates
 *
 */
public class OutputSampler {
    public static class SampleOutput {
        private final String schema;
        private final String output;

        /**
         * Constructs a SampleOutput from the given schema/output strings.
         *
         * @param schema
         * @param output
         */
        public SampleOutput(String schema, String output) {
            this.schema = schema;
            this.output = output;
        }

        public String getSchema() {
            return schema;
        }

        public String getOutput() {
            return output;
        }
    }

    private static final Log LOG = LogFactory.getLog(OutputSampler.class);
    public static final String DELIMITER = "\001";
    private final JobStats jobStats;

    /**
     * Constructs an OutputSampler from a JobStats object.
     *
     * @param jobStats
     */
    public OutputSampler(JobStats jobStats) {
        this.jobStats = jobStats;
    }

    /**
     * Check if StoreFunc is safe to sample data from.
     *
     * @param store the StoreFunc
     * @return true if safe to sample, otherwise false
     */
    protected boolean storeFuncValidForReading(StoreFuncInterface store) {
        return store instanceof InterStorage || store instanceof TFileStorage || store.getClass() == PigStorage.class;
    }

    /**
     * Returns a list of sample outputs limited by the maxRowsPerEntry and
     * maxBytesPerEntry.
     *
     * @param maxRowsPerEntry
     * @param maxBytesPerEntry
     * @return
     */
    public List<SampleOutput> getSampleOutputs(int maxRowsPerEntry, int maxBytesPerEntry) {
        List<SampleOutput> sampleOutputs = new LinkedList<SampleOutput>();

        for (POStore storeInfo : getStoreInfo(jobStats)) {
            LOG.info("Sample output: " + storeInfo);
            LOG.info("StoreFunc: " + storeInfo.getStoreFunc().getClass());
            if (storeInfo != null && storeFuncValidForReading(storeInfo.getStoreFunc())) {
                String schema = (storeInfo.getSchema() == null) ? ("") : storeInfo.getSchema().toString();
                sampleOutputs.add(new SampleOutput(schema, getSampleRows(storeInfo, maxRowsPerEntry, maxBytesPerEntry)));
            }
        }
        return sampleOutputs;
    }

    /* For a given object retrieve the value of a named field, regardless 
       of what class in the object's inheritance hierarchy the field was
       declared upon, and raises NoSuchFieldException if the field does
       not exist on any class in the hierarchy. */
    public Object getInheritedFieldValue(Object obj, String fieldName) throws IllegalAccessException, NoSuchFieldException {
        return getInheritedFieldValue(obj, obj.getClass(), fieldName);
    }

    protected Object getInheritedFieldValue(Object obj, Class cls, String fieldName) throws IllegalAccessException, NoSuchFieldException {
        try {
            Field f = cls.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.get(obj);
        } catch (NoSuchFieldException e) {
            Class souper = cls.getSuperclass();
            if (souper != null) {
                return getInheritedFieldValue(obj, souper, fieldName);
            } else {
                /* getSuperclass() returns null if we've gotten all the
                   way up to Object. At this point we've checked every class
                   in the heirarchy so the field must not exist. */
                throw new NoSuchFieldException(fieldName);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<POStore> getStoreInfo(JobStats jobStats) {
        List<POStore> storeInfo = new LinkedList<POStore>();

        // Use reflection to get the store info for the jobStats
        // Done b/c the OutputStats from jobStats.getOutputs()
        // doesn't include intermediate (temp) outputs
        List<POStore> mapStores = null;
        List<POStore> reduceStores = null;
        try {
            mapStores = (LinkedList<POStore>) getInheritedFieldValue(jobStats, "mapStores");
        } catch (Exception e) {
            LOG.warn("Failed to get map store information for jobId [" + jobStats.getJobId() + "].", e);
        }

        try {
            reduceStores = (LinkedList<POStore>) getInheritedFieldValue(jobStats, "reduceStores");
        } catch (Exception e) {
            LOG.warn("Failed to get reduce store information for jobId [" + jobStats.getJobId() + "].", e);
        }

        if (mapStores != null) {
            storeInfo.addAll(mapStores);
        } else {
            LOG.info("No map store information for jobId [" + jobStats.getJobId() + "].");
        }

        if (reduceStores != null) {
            storeInfo.addAll(reduceStores);
        } else {
            LOG.info("No reduce store information for jobId [" + jobStats.getJobId() + "].");
        }

        return storeInfo;
    }

    private String getSampleRows(POStore store, int maxRows, int maxBytes) {

        // Load the proper amount of data
        StringBuilder sb = new StringBuilder();

        try {
            LoadFunc loader = getLoader(store);
            if (loader != null) {
                int rowCount = 0;
                Tuple t = loader.getNext();
                while (t != null && rowCount < maxRows) {
                    String strTuple = t.toDelimitedString(DELIMITER);

                    if (strTuple != null) {
                        if (sb.length() + strTuple.length() + DELIMITER.length() > maxBytes) {
                            break;
                        }

                        if (sb.length() > 0) {
                            sb.append('\n');
                        }

                        sb.append(strTuple);
                    }
                    rowCount++;
                    t = loader.getNext();
                }
            }
        } catch (Exception e) {
            String sampleDescription = (sb.length() > 0) ? "full" : "any";
            LOG.warn("Unable to get " + sampleDescription + " sample for: " + store.getSFile(), e);
        }

        return sb.toString();
    }

    protected LoadFunc getLoader(POStore store) {
        // Create a loader from the POStore
        // Sampled from JobStats class
        LoadFunc loader = null;
        PigContext pigContext = ScriptState.get().getPigContext();
        try {
            LoadFunc originalLoadFunc = (LoadFunc) PigContext.instantiateFuncFromSpec(store.getSFile().getFuncSpec());

            loader = new ReadToEndLoader(originalLoadFunc,
                                         ConfigurationUtil.toConfiguration(pigContext.getProperties()),
                                         store.getSFile().getFileName(),
                                         0);

        } catch (Exception e) {
            LOG.warn("Unable to get sample rows for: " + store.getSFile(), e);
        }

        return loader;
    }
}
