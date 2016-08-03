package ch6.joins.composite;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by hua on 03/08/16.
 */
public class CompositeJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CompositeJoin(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path users = new Path(args[0]);
        Path userLogers = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "ComposteJoin");

        job.setMapperClass(Map.class);

        job.setInputFormatClass(CompositeInputFormat.class);

        job.getConfiguration().set(CompositeInputFormat.JOIN_EXPR,
                CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, users, userLogers));

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, userLogers);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static class Map extends Mapper<Text, TupleWritable, Text, Text> {
        @Override
        protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(StringUtils.join(value.get(0), value.get(1))));
        }
    }
}
