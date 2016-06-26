package smileyan.app.seqfile.writable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import smileyan.app.StockPriceWritable;

/**
 * Created by hua on 25/06/16.
 */
public class SequenceFileStockReader extends Configured implements Tool{

    // hadoop jar target/my-app-1.0-SNAPSHOT.jar smileyan.app.seqfile.writable.SequenceFileStockReader /tmp/hua/stocks.seqfile
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SequenceFileStockReader(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Path inputFile = new Path(args[0]);

        SequenceFile.Reader reader =   //<co id="ch03_comment_seqfile_read1"/>
                new SequenceFile.Reader(getConf(),
                        SequenceFile.Reader.file(inputFile));

        try {
            System.out.println("Is block compressed = " + reader.isBlockCompressed());

            Text key = new Text();
            StockPriceWritable value = new StockPriceWritable();

            while (reader.next(key, value)) {   //<co id="ch03_comment_seqfile_read2"/>
                System.out.println(key + "," + value);
            }
        } finally {
            reader.close();
        }
        return  0;
    }

}
