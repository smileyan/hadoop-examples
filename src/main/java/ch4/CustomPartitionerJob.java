package ch4;

import ch3.StockPriceWritable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

/**
 * Created by hua on 13/07/16.
 */
public class CustomPartitionerJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CustomPartitionerJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        addDatesToPartitions(getConf());

        Job job = Job.getInstance(getConf(), "Customer partition");
        job.setJarByClass(CustomPartitionerJob.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(DatePartitioner.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setNumReduceTasks(10);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text date = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StockPriceWritable stock =
                    StockPriceWritable.fromLine(value.toString());

            date.set(stock.getDate());
            context.write(date, value);
        }
    }

    public static void addDatesToPartitions(Configuration conf) {
        List<String> dates = Lists.newArrayList("2000-01-03",
                "2001-01-02", "2002-01-02", "2003-01-02", "2004-01-02",
                "2005-01-03", "2006-01-03", "2007-01-03", "2008-01-02",
                "2009-01-02");

        for (int partition = 0; partition < dates.size(); partition++) {
            String date = dates.get(partition);
            String addition = String.format("%s%s%d", date, DatePartitioner.PARTITION_DELIM, partition);

            String existing = conf.get(DatePartitioner.CONF_PARTITIONS);

            conf.set(DatePartitioner.CONF_PARTITIONS, existing == null ? addition : existing + "," + addition);
        }
    }

    public static class DatePartitioner extends Partitioner<Text, Text> implements Configurable {

        public static final String CONF_PARTITIONS = "partition.map";
        public static final String PARTITION_DELIM = ":";
        private Configuration conf;
        private java.util.Map<Text, Integer> datePartitions = Maps.newHashMap();

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            for (String partition : conf.getStrings(CONF_PARTITIONS)) {
                String[] parts = partition.split(PARTITION_DELIM);
                datePartitions.put(new Text(parts[0]), Integer.valueOf(parts[1]));
            }
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public int getPartition(Text key, Text value, int i) {
            return datePartitions.get(key);
        }
    }
}
