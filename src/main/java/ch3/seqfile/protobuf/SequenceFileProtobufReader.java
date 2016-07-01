package ch3.seqfile.protobuf;

import ch3.proto.StockProtos.Stock;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by hua on 1/07/16.
 */
public class SequenceFileProtobufReader extends Configured implements Tool{

    /*
     * hadoop jar myapp.jar ch3.seqfile.protobuf.SequenceFileProtobufReader /var/hadoop/output/*000
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileProtobufReader(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.printf("Usage: %s [generic options] <input>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Path inputFile = new Path(args[0]);

        ProtobufSerialization.register(getConf());

        SequenceFile.Reader reader =
                new SequenceFile.Reader(getConf(),
                        SequenceFile.Reader.file(inputFile));

        try {
            Text key = new Text();
            Stock value = Stock.getDefaultInstance();

            while (reader.next(key)) {
                value = (Stock) reader.getCurrentValue(value);
                System.out.println(key + "," + value);
            }
        } finally {
            reader.close();
        }

        return 0;
    }
}
