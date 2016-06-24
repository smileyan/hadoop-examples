package smileyan.app.json;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by hua on 24/06/16.
 */
public class JsonMapReduce extends Configured implements Tool {


    // # export HADOOP_CLASSPATH=~/.m2/repository/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar:$HADOOP_CLASSPATH
    // # hadoop jar target/my-app-1.0-SNAPSHOT.jar smileyan.app.json.JsonMapReduce input/test.json output
    // # hadoop jar target/my-app-1.0-S..jar -fs file:/// -jt local input/ output
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new JsonMapReduce(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "JsonMapReduce");

        job.setJarByClass(JsonMapReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setInputFormatClass(JsonInputFormat.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, MapWritable, Text, Text> {

        @Override
        protected void map(LongWritable key, MapWritable value, Context context)
                throws IOException, InterruptedException {

            for (java.util.Map.Entry<Writable, Writable> entry : value.entrySet()) {
                context.write((Text) entry.getKey(), (Text) entry.getValue());
            }
        }
    }
}
