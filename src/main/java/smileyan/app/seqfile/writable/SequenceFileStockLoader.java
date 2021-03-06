package smileyan.app.seqfile.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import ch3.StockPriceWritable;

import java.io.IOException;
import java.util.Arrays;

/**
 * REGISTER $HPDOOP_EXAMPLES/target/my-app-1.0-SNAPSHOT.jar
 * DEFINE SequenceFileStockLoader smileyan.app.seqfile.writable.SequenceFileStockLoader();
 * stocks = LOAD '/tmp/hua/stocks.seqfile' USING SequenceFileStockLoader;
 * dump stocks;
 */

public class SequenceFileStockLoader extends FileInputLoadFunc {

    private SequenceFileRecordReader<Text, StockPriceWritable> reader;

    @Override
    public Tuple getNext() throws IOException {
        boolean next;
        try {
            next = reader.nextKeyValue();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        if (!next) return null;

        Object value = reader.getCurrentValue();

        if (value == null) {
            return null;
        }
        if (!(value instanceof StockPriceWritable)) {
            return null;
        }
        StockPriceWritable w = (StockPriceWritable) value;

        return TupleFactory.getInstance().newTuple(Arrays.asList(
                w.getSymbol(), w.getDate(), w.getOpen(),
                w.getHigh(), w.getLow(), w.getClose(),
                w.getVolume(), w.getAdjClose()
        ));
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit pigSplit) throws IOException {
        this.reader = (SequenceFileRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new SequenceFileInputFormat<Text, StockPriceWritable>();
    }
}
