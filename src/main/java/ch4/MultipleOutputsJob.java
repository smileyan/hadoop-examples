package ch4;

import ch3.StockPriceWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by hua on 13/07/16.
 */
public class MultipleOutputsJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultipleOutputsJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "Multiple Output Job");
        job.setJarByClass(MultipleOutputsJob.class);
        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        MultipleOutputs.addNamedOutput(job, "partition", TextOutputFormat.class, Text.class, Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private MultipleOutputs outputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StockPriceWritable stock = StockPriceWritable.fromLine(value.toString());

            outputs.write(value, null, stock.getDate());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }
}
