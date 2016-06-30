package ch3.seqfile.protobuf;

import ch3.proto.StockProtos;
import ch3.proto.StockUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import smileyan.app.seqfile.writable.SequenceFileStockWriter;

import java.io.File;

/**
 * Created by hua on 30/06/16.
 */
public class SequenceFileProtobufWriter extends Configured implements Tool{

    /**
     * hip hip.ch3.seqfile.protobuf.SequenceFileProtobufWriter \
     --input test-data/stocks.txt \
     --output stocks.pb
     *
     * hadoop ch3.seqfile.protobuf.SequenceFileProtobufWriter test-data/stocks.txt /var/hadoop/stocks.pb
     * */

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

    public static void register(Configuration conf) {
        String[] serializations = conf.getStrings("io.serializations");
        if (ArrayUtils.isEmpty(serializations)) {
            serializations = new String[]{WritableSerialization.class.getName(),
                    AvroSpecificSerialization.class.getName(),
                    AvroReflectSerialization.class.getName()};
        }
        serializations = ArrayUtils.add(serializations, ProtobufSerialization.class.getName());
        conf.setStrings("io.serializations", serializations);
    }
}
