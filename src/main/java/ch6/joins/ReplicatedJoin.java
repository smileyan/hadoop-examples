package ch6.joins;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hua on 01/08/16.
 */
public class ReplicatedJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReplicatedJoin(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Path usersPath = new Path(args[0]);
        Path userLogsPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "ReplicatedJoin");

        job.setJarByClass(ReplicatedJoin.class);
        job.setMapperClass(JoinMap.class);

        job.addCacheFile(usersPath.toUri());
        job.getConfiguration().set(JoinMap.DISTCACHE_FILENAME_CONFIG, usersPath.getName());

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, userLogsPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static class JoinMap extends Mapper<LongWritable, Text, Text, Text> {
        public static final String DISTCACHE_FILENAME_CONFIG = "replicatedjoin.distcache.filename";
        private Map<String, User> users = new HashMap<String, User>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles();

            final String distributedCacheFilename = context.getConfiguration().get(DISTCACHE_FILENAME_CONFIG);

            boolean found = false;

            for(URI uri : files) {
                File path = new File(uri.getPath());

                if(path.getName().equals(distributedCacheFilename)) {
                    loadCache(path);
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new IOException("Unable to find file " + distributedCacheFilename);
            }
        }

        private void loadCache(File file) throws IOException {
            for (String line : FileUtils.readLines(file)) {
                User user = User.fromString(line);
                users.put(user.getName(), user);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            UserLog log = UserLog.fromText(value);

            User user = users.get(log.getName());

            if(user != null) {
                context.write(new Text(user.toString()), new Text(log.toString()));
            }
        }
    }
}
