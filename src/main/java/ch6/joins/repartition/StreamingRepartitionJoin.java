package ch6.joins.repartition;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by hua on 06/08/16.
 */
public class StreamingRepartitionJoin extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path usersPath = new Path(args[0]);
        Path userLogsPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "Stream repartition join");

        job.setJarByClass(StreamingRepartitionJoin.class);

        setPartitionerClass(job);
        setSortComparatorClass(job);
        setGroupingComparatorClass(job);

        MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserMap.class);
        MultipleInputs.addInputPath(job, userLogsPath, TextInputFormat.class, UserLogMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static class UserMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    public static class UserLogMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    // Partitioner controls the partitioning of the keys of the intermediate map-outputs.
    // The key (or a subset of the key) is used to derive the partition, typically by a hash function.
    // The total number of partitions is the same as the number of reduce tasks for the job.
    // Hence this controls which of the m reduce tasks the intermediate key (and hence the record) is sent for reduction
    public void setPartitionerClass(Job job) {
        job.setPartitionerClass(MyPartiton.class);
    }

    // Define the comparator that controls how the keys are sorted before they are passed to the Reducer.
    public void setSortComparatorClass(Job job) {
        job.setSortComparatorClass(MySortComparator.class);
    }

    // Define the comparator that controls which keys are grouped together for
    // a single call to Reducer.reduce(Object, Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    public void setGroupingComparatorClass(Job job) {
        job.setGroupingComparatorClass(MySortComparator.class);
    }

    public static class MyPartiton extends Partitioner {
        @Override
        public int getPartition(Object o, Object o2, int i) {
            if (i == 0) {
                return 0;
            }
            return 0;
        }
    }

    public static class MySortComparator extends WritableComparator {
        protected MySortComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;
            return -1 * key1.compareTo(key2);
        }
    }
}
