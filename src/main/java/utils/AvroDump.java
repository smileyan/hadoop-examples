package utils;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hua on 2/07/16.
 */
public class AvroDump extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroDump(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.printf("Usage: %s [generic options] <input>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Path inputFile = new Path(args[0]);

        FileSystem hdfs = FileSystem.get(getConf());

        InputStream is = hdfs.open(inputFile);
        readFromAvro(is);

        return 0;
    }

    public static void readFromAvro(InputStream is) throws IOException {
        DataFileStream<Object> reader =
                new DataFileStream<Object>(
                        is, new GenericDatumReader<Object>());
        for (Object o : reader) {
            System.out.println(o);
        }
        IOUtils.closeStream(is);
        IOUtils.closeStream(reader);
    }
}
