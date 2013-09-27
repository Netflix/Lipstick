package com.netflix.lipstick.util;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.service.HiveServer.HiveServerHandler;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.Counters.Counter;

import com.google.common.collect.Lists;
import com.netflix.lipstick.util.OutputSampler.SampleOutput;

public class HiveOutputSampler {
    private static final Log LOG = LogFactory.getLog(HiveOutputSampler.class);

    private final FileSinkOperator fsop;

    public HiveOutputSampler(FileSinkOperator fsop) {
        this.fsop = fsop;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<SampleOutput> getSampleOutputs(int maxRowsPerEntry, int maxBytesPerEntry) {
        List<SampleOutput> outputList = Lists.newArrayList();
        Path outputPath = new Path(fsop.getConf().getDirName());
        try {
            HiveConf hconf = new HiveConf(HiveServerHandler.class);
            FileSystem fs = outputPath.getFileSystem(hconf);

            if(fs.exists(outputPath)) {
                SequenceFileInputFormat inputFormat = new SequenceFileInputFormat();
                Deserializer des = fsop.getConf().getTableInfo().getDeserializer();
                JobConf jc = new JobConf(ExecDriver.class);
                jc.set("mapred.input.dir", outputPath.toString());

                InputSplit[] inputSplits = inputFormat.getSplits(jc, 1);

                if(inputSplits != null && inputSplits.length > 0) {
                    InputSplit is = inputSplits[0];
                    RecordReader rr = inputFormat.getRecordReader(is, jc, new MyReporter());
                    Writable key = new BytesWritable();
                    Writable value = new BytesWritable();

                    StringBuilder output = new StringBuilder();
                    int rowCount = 0;
                    while(rr.next(key, value) && rowCount < maxRowsPerEntry) {
                        StringBuilder row = new StringBuilder();
                        Object desVal = des.deserialize(value);
                        if(desVal instanceof LazyBinaryStruct) {
                            LazyBinaryStruct desValTyped = (LazyBinaryStruct)desVal;
                            for(Object fieldVal : desValTyped.getFieldsAsList()) {
                                if(row.length() > 0) {
                                    row.append(OutputSampler.DELIMITER);
                                }
                                String strFieldVal = (fieldVal == null) ? "NULL" : fieldVal.toString();
                                row.append(strFieldVal);
                            }
                            row.append('\n');
                        }

                        if(output.length() + row.length() <= maxBytesPerEntry) {
                            output.append(row.toString());
                        } else {
                            break;
                        }

                        rowCount++;
                    }
                    outputList.add(new SampleOutput(buildSchemaString(fsop.getSchema()), output.toString()));
                }
            }
        } catch(Exception e) {
            LOG.info("Caught exception getting Sample Output from [" + outputPath + "]", e);
        }

        return outputList;
    }

    private static String buildSchemaString(RowSchema rowSchema) {
        //{region: chararray,show_title_id1: long,show_title_id2: long,blended_mi: double}
        StringBuilder schemaBuilder = new StringBuilder();
        schemaBuilder.append('{');
        for(ColumnInfo ci : rowSchema.getSignature()) {
            if(schemaBuilder.length() > 1) {
                schemaBuilder.append(',');
            }
            String name = (ci.getAlias() != null) ? ci.getAlias() : ci.getInternalName();
            schemaBuilder.append(name + ": " + ci.getType().getTypeName());
        }
        schemaBuilder.append('}');
        return schemaBuilder.toString();
    }

    private static class MyReporter implements Reporter {
        @Override
        public void progress() {
        }

        @Override
        public Counter getCounter(Enum<?> arg0) {
            return null;
        }

        @Override
        public Counter getCounter(String arg0, String arg1) {
            return null;
        }

        @Override
        public InputSplit getInputSplit() throws UnsupportedOperationException {
            return null;
        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public void incrCounter(Enum<?> arg0, long arg1) {
        }

        @Override
        public void incrCounter(String arg0, String arg1, long arg2) {
        }

        @Override
        public void setStatus(String arg0) {
        }
    }
}
