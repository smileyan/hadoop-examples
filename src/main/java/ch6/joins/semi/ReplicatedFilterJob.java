package ch6.joins.semi;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by hua on 02/08/16.
 */
public class ReplicatedFilterJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReplicatedFilterJob(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path users = new Path(args[0]);
        Path uniqueUsers = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job = Job.getInstance(getConf(), "ReplicatedFilter Job");

        job.getConfiguration().set(Map.DISTCACHE_FILENAME_CONFIG, uniqueUsers.getName());
        job.addCacheFile(uniqueUsers.toUri());

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, users);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<Text, Text, Text, Text> {
        public static final String DISTCACHE_FILENAME_CONFIG = "replicatedjoin.distcache_1.filename";
        private Set<String> users = new HashSet<String>();

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
                users.add(line);
            }
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if(users.contains(key.toString())) {
                context.write(key, value);
            }
        }
    }



}
