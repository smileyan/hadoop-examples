package ch3.avro;

import ch3.avro.gen.Stock;
import ch3.avro.gen.StockAvg;
import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.avro.util.Utf8;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

/**
 * Created by hua on 05/07/16.
 */
public class AvroRecordMapReduce extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroRecordMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outoutPath = new Path(args[1]);

        JobConf job = new JobConf(getConf());
        job.setJarByClass(AvroRecordMapReduce.class);

        AvroJob.setInputSchema(job, Stock.SCHEMA$);
        AvroJob.setMapOutputSchema(job, Pair.getPairSchema(Schema.create(Schema.Type.STRING), Stock.SCHEMA$));
        AvroJob.setOutputSchema(job, StockAvg.SCHEMA$);

        AvroJob.setMapperClass(job, Mapper.class);
        AvroJob.setReducerClass(job, Reducer.class);

        FileOutputFormat.setCompressOutput(job, true);
        AvroJob.setOutputCodec(job, SNAPPY_CODEC);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outoutPath);

        return JobClient.runJob(job).isSuccessful() ? 0 : 1;
    }

    public static class Mapper extends AvroMapper<Stock, Pair<Utf8, Stock>> {

        @Override
        public void map(Stock datum, AvroCollector<Pair<Utf8, Stock>> collector, Reporter reporter) throws IOException {
            collector.collect(new Pair<Utf8, Stock>(new Utf8(datum.getSymbol().toString()), datum));
        }
    }

    public  static class Reducer extends AvroReducer<Utf8, Stock, StockAvg> {

        @Override
        public void reduce(Utf8 key, Iterable<Stock> values, AvroCollector<StockAvg> collector, Reporter reporter) throws IOException {
            Mean mean = new Mean();

            for (Stock s: values) {
                mean.increment(s.getOpen());
            }

            StockAvg avg = new StockAvg();
            avg.setSymbol(key.toString());
            avg.setAvg(mean.getResult());

            collector.collect(avg);
        }
    }
}
