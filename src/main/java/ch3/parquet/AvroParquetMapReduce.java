package ch3.parquet;

import ch3.avro.gen.Stock;
import ch3.avro.gen.StockAvg;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

/**
 * Created by hua on 10/07/16.
 */
public class AvroParquetMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroParquetMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "Avro Parquet Map Reduce");
        job.setJarByClass(AvroParquetMapReduce.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, inputPath);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, StockAvg.SCHEMA$);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<Void, Stock, Text, DoubleWritable> {
        @Override
        protected void map(Void key, Stock value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.getSymbol().toString()),
                    new DoubleWritable(value.getOpen()));
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Void, StockAvg> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Mean mean = new Mean();
            for (DoubleWritable value : values) {
                mean.increment(value.get());
            }

            StockAvg avg = new StockAvg();
            avg.setAvg(mean.getResult());
            avg.setSymbol(key.toString());

            context.write(null, avg);
        }
    }
}
