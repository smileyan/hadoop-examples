package ch3.avro;

import ch3.avro.gen.Stock;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
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
 * Created by hua on 06/07/16.
 */
public class AvroKeyValueFileWrite extends Configured implements Tool{


    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AvroKeyValueFileWrite(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        File inputFile = new File(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(getConf());

        OutputStream outputStream = fs.create(outputPath);
        writeToAvro(inputFile, outputStream);

        return 0;
    }

    public static Schema SCHEMA  = AvroKeyValue.getSchema(Schema.create(Schema.Type.STRING), Stock.SCHEMA$);

    public static void writeToAvro(File inputFile, OutputStream os) throws IOException{
        DataFileWriter<GenericRecord> writer =
                new DataFileWriter<GenericRecord>(
                        new GenericDatumWriter<GenericRecord>());

        writer.setCodec(CodecFactory.snappyCodec());
        writer.create(SCHEMA, os);

        for (Stock stock : AvroStockUtils.fromCsvFile(inputFile)) {

            AvroKeyValue<CharSequence, Stock> record
                    = new AvroKeyValue<CharSequence, Stock>(new GenericData.Record(SCHEMA));
            record.setKey(stock.getSymbol());
            record.setValue(stock);

            writer.append(record.get());
        }

        IOUtils.closeStream(writer);
        IOUtils.closeStream(os);
    }
}
