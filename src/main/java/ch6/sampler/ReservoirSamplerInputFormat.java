package ch6.sampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created by hua on 29/08/16.
 */
public class ReservoirSamplerInputFormat<K extends Writable, V extends Writable> extends InputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return getInputFormat(jobContext.getConfiguration()).getSplits(jobContext);
    }


    private InputFormat<K, V> inputFormat;

    public static final String INPUT_FORMAT_CLASS = "reservoir.inputformat.class";

    @SuppressWarnings("unchecked")
    public InputFormat<K, V> getInputFormat(Configuration conf)
            throws IOException {

        if (inputFormat == null) {
            Class ifClass = conf.getClass(INPUT_FORMAT_CLASS, null);

            if (ifClass == null) {
                throw new IOException("Job must be configured with " + INPUT_FORMAT_CLASS);
            }

            inputFormat = (InputFormat<K, V>) ReflectionUtils.newInstance(ifClass, conf);
        }

        return inputFormat;
    }
}
