package ch3.seqfile.protobuf;

import ch3.proto.StockProtos;
import ch3.proto.StockUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

import static ch3.seqfile.protobuf.ProtobufSerialization.register;

/**
 * Created by hua on 30/06/16.
 */
public class SequenceFileProtobufWriter extends Configured implements Tool{

    /**
     * hadoop ch3.seqfile.protobuf.SequenceFileProtobufWriter test-data/stocks.txt /var/hadoop/stocks.pb
     */

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileProtobufWriter(), args);
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

        register(getConf());

        SequenceFile.Writer writer =
                SequenceFile.createWriter(getConf(),
                        SequenceFile.Writer.file(outputPath),
                        SequenceFile.Writer.keyClass(Text.class),
                        SequenceFile.Writer.valueClass(StockProtos.Stock.class),
                        SequenceFile.Writer.compression(
                                SequenceFile.CompressionType.BLOCK,
                                new DefaultCodec()));

        try {
            Text key = new Text();

            for (StockProtos.Stock stock : StockUtils.fromCsvFile(inputFile)) {
                key.set(stock.getSymbol());
                writer.append(key, stock);
            }
        } finally {
            writer.close();
        }

        return 0;
    }


}
