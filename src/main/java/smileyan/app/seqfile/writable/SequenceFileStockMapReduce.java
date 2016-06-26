package smileyan.app.seqfile.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import smileyan.app.StockPriceWritable;

/**
 * Created by hua on 26/06/16.
 */
public class SequenceFileStockMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileStockMapReduce(), args);
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

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf,"SequenceFileStackMapReduce");
        job.setJarByClass(SequenceFileStockMapReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StockPriceWritable.class);
        job.setInputFormatClass(
                SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job,
                SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job,
                DefaultCodec.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
}
