package ch3.avro;

import ch3.avro.gen.Stock;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
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
public class AvroStockFileRead extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroStockFileRead(), args);
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
        dumpStream(is);

        return 0;
    }

    public static void dumpStream(InputStream is) throws IOException {
        DataFileStream<Stock> reader =
                new DataFileStream<Stock>(
                        is,
                        new SpecificDatumReader<Stock>(Stock.class));

        for (Stock a : reader) {
            System.out.println(ToStringBuilder.reflectionToString(a,
                    ToStringStyle.SIMPLE_STYLE
            ));
        }

        IOUtils.closeStream(is);
        IOUtils.closeStream(reader);
    }
}
