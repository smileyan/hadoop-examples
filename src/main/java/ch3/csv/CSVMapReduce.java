package ch3.csv;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by hua on 12/07/16.
 */
public class CSVMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CSVMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        getConf().set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");
        getConf().set(CSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ":");

        Job job = Job.getInstance(getConf(), "CSV map reduce");
        job.setJarByClass(CSVMapReduce.class);

        job.setInputFormatClass(CSVInputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);


        job.setOutputFormatClass(CSVOutputFormat.class);
        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, TextArrayWritable, LongWritable, TextArrayWritable> {

        @Override
        protected void map(LongWritable key, TextArrayWritable value, Mapper.Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Reduce extends Reducer<LongWritable, TextArrayWritable, TextArrayWritable, NullWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val : values) {
                context.write(val, NullWritable.get());
            }
        }
    }

}
