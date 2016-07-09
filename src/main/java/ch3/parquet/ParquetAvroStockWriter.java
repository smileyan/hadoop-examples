package ch3.parquet;

import ch3.avro.gen.Stock;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;

import static ch3.avro.AvroStockUtils.fromCsvFile;

/**
 * Created by hua on 09/07/16.
 */
public class ParquetAvroStockWriter extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParquetAvroStockWriter(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        File inputFile = new File(args[0]);
        Path outputPath = new Path(args[1]);

        AvroParquetWriter<Stock> writer = new AvroParquetWriter<Stock>(
                outputPath,
                Stock.SCHEMA$,
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                true);

        for (Stock stock : fromCsvFile(inputFile)) {
            writer.write(stock);
        }

        writer.close();

        return 0;
    }
}
