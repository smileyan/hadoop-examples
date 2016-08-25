package ch6.sort.secondary;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by hua on 25/08/16.
 */
public class SortMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(SortMapReduce.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Person.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(PersonNamePartitioner.class);
        job.setSortComparatorClass(PersonComparator.class);
        job.setGroupingComparatorClass(PersonNameComparator.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static class Map
            extends Mapper<Text, Text, Person, Text> {

    }

    public static class Reduce
            extends Reducer<Person, Text, Text, Text> {

    }
}
