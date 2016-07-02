package ch3.avro;

import ch3.avro.gen.Stock;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by hua on 2/07/16.
 */
public class AvroStockFileWrite extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroStockFileWrite(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args ) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        File inputFile = new File(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem hdfs = FileSystem.get(getConf());

        OutputStream os = hdfs.create(outputPath);
        writeToAvro(inputFile, os);

        return 0;
    }

    public static void writeToAvro(File inputFile, OutputStream outputStream)
            throws IOException {

        DataFileWriter<Stock> writer =
                new DataFileWriter<Stock>(
                        new SpecificDatumWriter<Stock>());

        writer.setCodec(CodecFactory.snappyCodec());
        writer.create(Stock.SCHEMA$, outputStream);

        for (Stock stock : AvroStockUtils.fromCsvFile(inputFile)) {
            writer.append(stock);
        }

        IOUtils.closeStream(writer);
        IOUtils.closeStream(outputStream);
    }
}
