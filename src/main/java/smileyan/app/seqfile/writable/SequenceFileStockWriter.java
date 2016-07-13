package smileyan.app.seqfile.writable;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch3.StockPriceWritable;

import java.io.File;
import java.io.IOException;

/**
 * Created by hua on 25/06/16.
 */
public class SequenceFileStockWriter extends Configured implements Tool{

    // hadoop jar target/my-app-1.0-SNAPSHOT.jar smileyan.app.seqfile.writable.SequenceFileStockWriter \
    //                                  ~/projects/hiped2/test-data/stocks.txt \
    //                                  /tmp/hua/stocks.seqfile
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileStockWriter(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        File inputFile = new File(args[0]);
        Path outputPath = new Path(args[1]);

        SequenceFile.Writer writer = SequenceFile.createWriter(getConf(),
                SequenceFile.Writer.file(outputPath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(StockPriceWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()));

        try {
            Text key = new Text();

            for (String line : FileUtils.readLines(inputFile)) {
                StockPriceWritable stock = fromLine(line);

                key.set(stock.getSymbol());

                writer.append(key, stock);
            }
        } finally {
            writer.close();
        }
        return 0;
    }

    public static StockPriceWritable fromLine(String line)
            throws IOException {
        String[] parts = line.split(",");

        StockPriceWritable stock = new StockPriceWritable(
                parts[0], parts[1], Double.valueOf(parts[2]),
                Double.valueOf(parts[3]),
                Double.valueOf(parts[4]),
                Double.valueOf(parts[5]),
                Integer.valueOf(parts[6]),
                Double.valueOf(parts[7])
        );
        return stock;
    }
}
