package ch3.parquet;

import ch3.avro.gen.Stock;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Created by hua on 09/07/16.
 */
public class ParquetAvroStockReader extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParquetAvroStockReader(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);

        ParquetReader<Stock> reader = AvroParquetReader.builder(inputPath).build();

        Stock stock;
        while ((stock = reader.read()) != null) {
            System.out.println(ToStringBuilder.reflectionToString(stock,
                    ToStringStyle.SIMPLE_STYLE));
        }

        reader.close();

        return 0;
    }
}
