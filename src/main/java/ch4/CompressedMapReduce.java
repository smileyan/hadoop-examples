package ch4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by hua on 15/07/16.
 */
public class CompressedMapReduce {

    public static void main(String... args) throws Exception {

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Compressed map reduce");
        job.setJarByClass(CompressedMapReduce.class);

        Class<?> codecClass = Class.forName(args[2]);

        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec",
                codecClass, CompressionCodec.class);

        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec",
                codecClass, CompressionCodec.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);


        job.waitForCompletion(true);
    }
}
