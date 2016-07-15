package ch4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.InputStream;

/**
 * Created by hua on 15/07/16.
 */
public class CompressedFileRead {
    public static void main(String... args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Class<?> codecClass = Class.forName(args[1]);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        InputStream in = codec.createInputStream(hdfs.open(new Path(args[0])));

        IOUtils.copyBytes(in, System.out, conf, true);

        IOUtils.closeStream(in);

    }
}
