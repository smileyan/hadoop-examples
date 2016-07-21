package ch5;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.OutputStream;

/**
 * Created by hua on 20/07/16.
 */
public class CopyStreamToHdfs extends Configured implements Tool {

    public static void main(String... args) throws Exception {
        int exitCode = ToolRunner.run(new CopyStreamToHdfs(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path out = new Path(args[0]);


        FileSystem hdfs = FileSystem.get(getConf());

        OutputStream os = null;

        try {
            os = hdfs.create(out);
            IOUtils.copyBytes(System.in, os, getConf(), false);
        } finally {
            IOUtils.closeStream(os);
        }

        return 0;
    }

}
