package ch6.joins.repartition;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;

/**
 * Created by hua on 06/08/16.
 */
public class StreamingRepartitionJoin extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Stream repartition join");

        setPartitionerClass(job);


        return job.waitForCompletion(true)? 0 : 1;
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
