package ch4;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by hua on 16/07/16.
 */
public class LzopFileReadWrite extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        int exitCode = ToolRunner.run(new LzopFileReadWrite(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path srcFile = new Path(args[0]);
        Path restoredFile = new Path(args[0] + ".restored");

        Path destFile = compress(srcFile, getConf());
        decompress(destFile, restoredFile, getConf());

        return 0;
    }

    public static Path compress(Path src, Configuration conf) throws IOException {
        LzopCodec codec = new LzopCodec();
        codec.setConf(conf);

        Path dest = new Path(src.getName() + codec.getDefaultExtension());

        FileSystem hdfs = FileSystem.get(conf);

        InputStream in = null;
        OutputStream out = null;

        try{
            in = hdfs.open(src);
            out = codec.createOutputStream(hdfs.create(dest));

            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

        return dest;
    }

    public static void decompress(Path src, Path dest, Configuration conf) throws IOException {
        LzopCodec codec = new LzopCodec();
        codec.setConf(conf);

        FileSystem hdfs = FileSystem.get(conf);
        InputStream in = null;
        OutputStream out = null;

        try {
            in = hdfs.open(src);
            out = hdfs.create(dest);

            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
