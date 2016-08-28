package ch6.sort.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by hua on 28/08/16.
 */
public class TotalSortMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TotalSortMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path partitionFile = new Path(args[1]);
        Path output = new Path(args[2]);

        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(TotalSortMapReduce.class);

        job.setNumReduceTasks(2);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TotalOrderPartitioner.setPartitionFile(getConf(), partitionFile);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        InputSampler.writePartitionFile(job, sampler);

        URI partitionUri = new URI(partitionFile.toString() +
            "#" + "_sortPartitioning");
        job.addCacheFile(partitionUri);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
}
