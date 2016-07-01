package ch3.seqfile.protobuf;

import ch3.proto.StockProtos;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static ch3.seqfile.protobuf.ProtobufSerialization.register;

/**
 * Created by hua on 1/07/16.
 */
public class SequenceFileProtobufMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileProtobufMapReduce(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(SequenceFileProtobufMapReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StockProtos.Stock.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(PbMapper.class);
        job.setReducerClass(PbReducer.class);

        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        register(job.getConfiguration());

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) == true ? 0 : 1;

    }

    public static class PbMapper extends Mapper<Text, StockProtos.Stock, Text, StockProtos.Stock> {
        @Override
        protected void map(Text key, StockProtos.Stock value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class PbReducer extends Reducer<Text, StockProtos.Stock, Text, StockProtos.Stock> {

        @Override
        protected void reduce(Text symbol, Iterable<StockProtos.Stock> values, Context context) throws IOException, InterruptedException {
            for (StockProtos.Stock stock : values) {
                context.write(symbol, stock);
            }
        }
    }
}
