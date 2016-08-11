package ch6.joins.bloom;

import ch6.joins.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
        private BloomFilter filter = new BloomFilter(1000, 5, Hash.MURMUR_HASH);

        @Override
        protected void reduce(NullWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            for (BloomFilter bf: values) {
                filter.or(bf);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new AvroKey<GenericRecord>(toGenericRecord(filter)), NullWritable.get());
        }

        public static final String BYTES_FIELD = "b";

        private static final String SCHEMA_JSON =
                "{\"type\": \"record\", \"name\": \"Bytes\", "
                        + "\"fields\": ["
                        + "{\"name\":\"" + BYTES_FIELD
                        + "\", \"type\":\"bytes\"}]}";
        public static final Schema SCHEMA = Schema.parse(SCHEMA_JSON);

        public static GenericRecord toGenericRecord(Writable writable)
                throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dao = new DataOutputStream(baos);
            writable.write(dao);
            dao.close();
            return toGenericRecord(baos.toByteArray());
        }

        public static GenericRecord toGenericRecord(byte[] bytes) {
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put(BYTES_FIELD, ByteBuffer.wrap(bytes));
            return record;
        }
    }
}
