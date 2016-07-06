package ch3.avro;

import ch3.avro.gen.Stock;
import ch3.avro.gen.StockAvg;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by hua on 06/07/16.
 */
public class AvroKeyValueMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroKeyValueMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "Avro Key Value Map Reduce");
        job.setJarByClass(AvroKeyValueMapReduce.class);

        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Stock.SCHEMA$);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvroValue.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputValueSchema(job, StockAvg.SCHEMA$);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<AvroKey<CharSequence>, AvroValue<Stock>, Text, DoubleWritable> {
        @Override
        protected void map(AvroKey<CharSequence> key, AvroValue<Stock> value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()),
                    new DoubleWritable(value.datum().getOpen()));
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, AvroValue<StockAvg>> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Mean mean = new Mean();
            for (DoubleWritable val: values) {
                mean.increment(val.get());
            }
            StockAvg avg = new StockAvg();
            avg.setSymbol(key.toString());
            avg.setAvg(mean.getResult());
            context.write(key, new AvroValue<StockAvg>(avg));
        }
    }
}
