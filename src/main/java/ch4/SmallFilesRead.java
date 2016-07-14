package ch4;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by hua on 14/07/16.
 */
public class SmallFilesRead extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesRead(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(getConf());

        Path input = new Path(args[0]);

        InputStream is = fs.open(input);

        readFromAvro(is);

        return 0;
    }

    private static final String FIELD_FILENAME = "filename";
    private static final String FIELD_CONTENTS = "contents";

    public static void readFromAvro(InputStream is) throws IOException {
        DataFileStream<Object> reader = new DataFileStream<Object>(is, new GenericDatumReader<Object>());

        for (Object o : reader) {
            GenericRecord record = (GenericRecord) o;
            System.out.println(
                    record.get(FIELD_FILENAME) +
                            ": " +
                            DigestUtils.md5Hex(
                                    ((ByteBuffer) record.get(FIELD_CONTENTS)).array()));
        }

        IOUtils.closeStream(is);
        IOUtils.cleanup(null, reader);
    }
}
