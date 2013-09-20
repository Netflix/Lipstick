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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.io.InterStorage.InterInputFormat;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.JobStats;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.netflix.lipstick.util.OutputSampler;
import com.netflix.lipstick.util.OutputSampler.SampleOutput;

public class OutputSamplerTest {

    @Test
    public void getSampleOutputAllOutputLessThanMaxBytesAndRows() throws Exception {
        final String output = "testRecord";
        final String schema = "{blah: chararray}";

        final InterStorage is = getInterStorage(Lists.newArrayList(output));
        List<SampleOutput> outputs = getSampleOutputs(is, schema);

        Assert.assertEquals(outputs.get(0).getOutput(), output);
        Assert.assertEquals(outputs.get(0).getSchema(), schema);
    }

    @Test
    public void getSampleOutputAllOutputMoreThanMaxBytes() throws Exception {
        final String schema = "{blah: chararray}";
        int recordCount = 1000;
        int byteCount = 256;

        final InterStorage is = getInterStorage(createRecords(recordCount));
        List<SampleOutput> outputs = getSampleOutputs(is, schema, recordCount, byteCount);

        Assert.assertTrue(outputs.get(0).getOutput().split("\n").length < recordCount);
        Assert.assertTrue(outputs.get(0).getOutput().length() <= byteCount);
        Assert.assertEquals(outputs.get(0).getSchema(), schema);
    }

    @Test
    public void getSampleOutputAllOutputMoreThanMaxRows() throws Exception {
        final String schema = "{blah: chararray}";
        int recordCount = 10;
        int byteCount = 1024;

        final InterStorage is = getInterStorage(createRecords(recordCount));
        List<SampleOutput> outputs = getSampleOutputs(is, schema, recordCount, byteCount);

        Assert.assertEquals(outputs.get(0).getOutput().split("\n").length, recordCount);
        Assert.assertTrue(outputs.get(0).getOutput().length() <= byteCount);
        Assert.assertEquals(outputs.get(0).getSchema(), schema);
    }

    @Test
    public void getSampleOutputFirstRowLargerThanMaxBytes() throws Exception {
        final String output = "This record is larger than our byte count and we expect no output";
        final String schema = "{blah: chararray}";
        int recordCount = 10;
        int byteCount = 20;

        final InterStorage is = getInterStorage(Lists.newArrayList(output));
        List<SampleOutput> outputs = getSampleOutputs(is, schema, recordCount, byteCount);

        Assert.assertEquals(outputs.get(0).getOutput().length(), 0);
        Assert.assertEquals(outputs.get(0).getSchema(), schema);
    }

    private List<SampleOutput> getSampleOutputs(InterStorage is, String schema) throws Exception {
        return getSampleOutputs(is, schema, 10, 1024);
    }

    private List<SampleOutput> getSampleOutputs(InterStorage is, String schema, int recordCount, int byteCount)
            throws Exception {
        POStore pos = getPOStore(is, schema);
        List<POStore> posList = Lists.newLinkedList();
        posList.add(pos);

        JobStats js = getJobStats();
        addStoresToJobStats(js, posList);

        OutputSampler os = getOutputSamplerWithOverriddenLoader(is, js);
        return os.getSampleOutputs(recordCount, byteCount);
    }

    private List<String> createRecords(int recordCount) {
        final String output = "testRecord";
        List<String> recordList = new LinkedList<String>();
        for (int i = 0; i < recordCount; i++) {
            String record = output + i;
            recordList.add(record);
        }
        return recordList;
    }

    private OutputSampler getOutputSamplerWithOverriddenLoader(final InterStorage is, JobStats js) {
        OutputSampler os = new OutputSampler(js) {
            @Override
            protected LoadFunc getLoader(POStore pos) {
                return is;
            }
        };
        return os;
    }

    private JobStats getJobStats() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {
        Constructor<?> ctor = JobStats.class.getDeclaredConstructors()[0];
        ctor.setAccessible(true);
        return (JobStats) ctor.newInstance(null, null);
    }

    private POStore getPOStore(InterStorage is, String schema) throws IOException {
        POStore pos = mock(POStore.class);
        when(pos.getStoreFunc()).thenReturn(is);
        when(pos.getSchema()).thenReturn(new Schema(new Schema.FieldSchema("blah", (byte) 55)));
        return pos;
    }

    private InterStorage getInterStorage(List<String> returnRecords) throws IOException {
        InterStorage is = mock(InterStorage.class);
        when(is.getInputFormat()).thenReturn(new InterInputFormat());

        List<Tuple> tuples = new LinkedList<Tuple>();
        for (String record : returnRecords) {
            Tuple tup = mock(Tuple.class);
            when(tup.toDelimitedString(OutputSampler.DELIMITER)).thenReturn(record);
            tuples.add(tup);
        }
        tuples.add(null);
        when(is.getNext()).thenReturn(tuples.remove(0), tuples.toArray(new Tuple[0]));

        return is;
    }

    private void addStoresToJobStats(JobStats js, List<POStore> stores) throws SecurityException, NoSuchFieldException,
            IllegalArgumentException, IllegalAccessException {
        Field f = js.getClass().getDeclaredField("mapStores");
        f.setAccessible(true);
        f.set(js, stores);
    }

    class BaseClass {
        public String BaseClsAttr = "foo";
    }

    class ChildClass extends BaseClass {};

    @Test
    public void testGetInherittedFieldValue() throws Exception {
        OutputSampler sampler = new OutputSampler(null);
        ChildClass obj = new ChildClass();
        String value = (String)sampler.getInherittedFieldValue(obj, "BaseClsAttr");
        Assert.assertEquals("foo", value);
    }
    
}
