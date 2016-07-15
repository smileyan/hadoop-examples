package ch4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by hua on 15/07/16.
 */
public class CompressedFileWrite {
    public static void main(String... args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Class<?> codecClass = Class.forName(args[1]);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        InputStream is = hdfs.open(new Path(args[0]));

        OutputStream os = hdfs.create(new Path(args[0] + codec.getDefaultExtension()));
        OutputStream osc = codec.createOutputStream(os);

        IOUtils.copyBytes(is, osc, conf, true);

        IOUtils.closeStream(is);
        IOUtils.closeStream(os);
    }
}
