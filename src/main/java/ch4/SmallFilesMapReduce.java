package ch4;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by hua on 14/07/16.
 */
public class SmallFilesMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "small files map reduce");
        job.setJarByClass(SmallFilesMapReduce.class);

        getConf().set(AvroJob.INPUT_SCHEMA, SmallFilesWrite.SCHEMA.toString());

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<AvroWrapper<GenericRecord>, NullWritable, Text, Text>{

        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(AvroWrapper<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {

            k.set(key.datum().get(SmallFilesWrite.FIELD_FILENAME).toString());
            v.set(DigestUtils.md5Hex(
                    ((ByteBuffer) key.datum().get(SmallFilesWrite.FIELD_CONTENTS))
                            .array()));

            context.write(k, v);
        }
    }
}
