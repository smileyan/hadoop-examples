package ch3.parquet;

import ch3.avro.gen.Stock;
import ch3.avro.gen.StockAvg;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.ColumnPredicates;
import org.apache.parquet.filter.ColumnRecordFilter;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;

import java.io.IOException;
import java.util.List;

/**
 * Created by hua on 10/07/16.
 */
public class AvroProjectionParquetMapReduce extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroProjectionParquetMapReduce(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(AvroProjectionParquetMapReduce.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, inputPath);

        // predicate pushdown
        AvroParquetInputFormat.setUnboundRecordFilter(job, GoogleStockFilter.class);

        AvroParquetInputFormat.setRequestedProjection(job, getProjection());

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, StockAvg.SCHEMA$);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static Schema getProjection() {

        // projection pushdown
        Schema projection = Schema.createRecord(Stock.SCHEMA$.getName(),
                Stock.SCHEMA$.getDoc(), Stock.SCHEMA$.getNamespace(), false);
        List<Schema.Field> fields = Lists.newArrayList();
        for (Schema.Field field : Stock.SCHEMA$.getFields()) {
            if ("symbol".equals(field.name()) || "open".equals(field.name())) {
                fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
                        field.defaultValue(), field.order()));
            }
        }

        projection.setFields(fields);

        return projection;
    }


    public static class GoogleStockFilter implements UnboundRecordFilter {

        private final UnboundRecordFilter filter;

        public GoogleStockFilter() {
            filter = ColumnRecordFilter.column("symbol", ColumnPredicates.equalTo("GOOG"));
        }

        @Override
        public RecordFilter bind(Iterable<ColumnReader> readers) {
            return filter.bind(readers);
        }
    }

    public static class Map extends Mapper<Void, Stock, Text, DoubleWritable> {

        @Override
        public void map(Void key,
                        Stock value,
                        Context context) throws IOException, InterruptedException {
            // the stock value can be null due to predicate pushdown
            if (value != null) {
                context.write(new Text(value.getSymbol().toString()),
                        new DoubleWritable(value.getOpen()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Void, StockAvg> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Mean mean = new Mean();
            for (DoubleWritable val : values) {
                mean.increment(val.get());
            }
            StockAvg avg = new StockAvg();
            avg.setSymbol(key.toString());
            avg.setAvg(mean.getResult());
            context.write(null, avg);
        }
    }
}
