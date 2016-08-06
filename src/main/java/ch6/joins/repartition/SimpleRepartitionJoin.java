package ch6.joins.repartition;

import ch6.joins.Tuple;
import ch6.joins.User;
import ch6.joins.UserLog;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.groovy.ast.expr.TupleExpression;

import java.io.IOException;
import java.util.List;

/**
 * Created by hua on 04/08/16.
 */
public class SimpleRepartitionJoin extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {

        Path userPath = new Path(args[0]);
        Path userLogPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "Simple repartition join");

        job.setJarByClass(SimpleRepartitionJoin.class);

        MultipleInputs.addInputPath(job, userPath,
                TextInputFormat.class, UserMap.class);
        MultipleInputs.addInputPath(job, userLogPath,
                TextInputFormat.class, UserLogMap.class);

        job.setReducerClass(Reduce.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1 ;
    }

    public static class UserMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            User user = User.fromText(value);

            Tuple tuple = new Tuple();
            tuple.setInt(0);
            tuple.setString(value.toString());

            context.write(new Text(user.getName()), tuple.toText());
        }
    }

    public static class UserLogMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            UserLog userLog = UserLog.fromText(value);

            Tuple tuple = new Tuple();
            tuple.setInt(1);
            tuple.setString(value.toString());

            context.write(new Text(userLog.getName()), tuple.toText());
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        List<String> users;
        List<String> userLogs;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            users = Lists.newArrayList();
            userLogs = Lists.newArrayList();

            for (Text text : values) {
                Tuple tuple = new Tuple();
                tuple.setText(text);

                if (tuple.getInt() == 0)
                    users.add(tuple.getString());
                else
                    userLogs.add(tuple.getString());
            }

            for (String user: users) {
                for (String userLog: userLogs) {
                    context.write(new Text(user), new Text(userLog));
                }
            }
        }
    }

}
