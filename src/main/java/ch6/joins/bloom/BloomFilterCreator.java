package ch6.joins.bloom;

import ch6.joins.User;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;

/**
 * Created by hua on 08/08/16.
 */
public class BloomFilterCreator extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new BloomFilterCreator(), args);
        System.exit(exitcode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "bloom fiter ");

        job.setJarByClass(BloomFilterCreator.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        return 0;
    }

    public static class Map extends Mapper<LongWritable, Text, NullWritable, BloomFilter> {
        private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            User user = User.fromText(value);

            if ("CA".equals(user.getState())) {
                filter.add(new Key(user.getName().getBytes()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), filter);
        }
    }

    public static class Reduce extends Reducer<NullWritable, BloomFilter, AvroKey<GenericRecord>, NullWritable> {

    }
}
