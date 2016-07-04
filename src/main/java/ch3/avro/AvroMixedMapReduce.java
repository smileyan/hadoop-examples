package ch3.avro;

import ch3.avro.gen.Stock;
import ch3.avro.gen.StockAvg;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by hua on 03/07/16.
 */
public class AvroMixedMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroMixedMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        JobConf job = new JobConf(getConf());
        job.setJarByClass(AvroMixedMapReduce.class);

        job.set(AvroJob.INPUT_SCHEMA, Stock.SCHEMA$.toString());
        job.set(AvroJob.OUTPUT_SCHEMA, StockAvg.SCHEMA$.toString());
        job.set(AvroJob.OUTPUT_CODEC, SnappyCodec.class.getName());

        job.setInputFormat(AvroInputFormat.class);
        job.setOutputFormat(AvroOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return JobClient.runJob(job).isSuccessful() ? 0 : 1;

    }

    public static class Map extends MapReduceBase
            implements
            Mapper<AvroWrapper<Stock>, NullWritable, Text, DoubleWritable> {

        @Override
        public void map(AvroWrapper<Stock> key,
                        NullWritable nullWritable,
                        OutputCollector<Text, DoubleWritable> output,
                        Reporter reporter) throws IOException {
            output.collect(new Text(key.datum().getSymbol().toString()),
                    new DoubleWritable(key.datum().getOpen()));
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, DoubleWritable, AvroWrapper<StockAvg>,
            NullWritable> {

        @Override
        public void reduce(Text key,
                           Iterator<DoubleWritable> values,
                           OutputCollector<AvroWrapper<StockAvg>, NullWritable> output,
                           Reporter reporter) throws IOException {
            Mean mean = new Mean();
            while (values.hasNext()) {
                mean.increment(values.next().get());
            }
            StockAvg avg = new StockAvg();
            avg.setSymbol(key.toString());
            avg.setAvg(mean.getResult());
            output.collect(new AvroWrapper<StockAvg>(avg),
                    NullWritable.get());
        }
    }

}
