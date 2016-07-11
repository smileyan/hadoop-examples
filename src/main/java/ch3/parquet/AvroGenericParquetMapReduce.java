package ch3.parquet;

import ch3.avro.gen.Stock;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by hua on 10/07/16.
 */
public class AvroGenericParquetMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroGenericParquetMapReduce(), args);
        System.exit(exitCode);
    }
    private static Schema avroSchema;

    static {
        avroSchema = Schema.createRecord("TestRecord", null, null, false);
        avroSchema.setFields(
                Arrays.asList(new Schema.Field("foo",
                        Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL))),
                        null, null)));
    }

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(AvroGenericParquetMapReduce.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setInputPaths(job, inputPath);

        Schema schema = Schema.createRecord("foobar",
                Stock.SCHEMA$.getDoc(), Stock.SCHEMA$.getNamespace(), false);
        List<Schema.Field> fields = Lists.newArrayList();
        for (Schema.Field field : Stock.SCHEMA$.getFields()) {
            fields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
                    field.defaultValue(), field.order()));
        }
        schema.setFields(fields);

        AvroParquetInputFormat.setAvroReadSchema(job, schema);

        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<Void, GenericRecord, Void, GenericRecord> {

        @Override
        public void map(Void key,
                        GenericRecord value,
                        Context context) throws IOException, InterruptedException {
            GenericRecord output = new GenericRecordBuilder(avroSchema)
                    .set("foo", value.get("open"))
                    .build();

            context.write(key, output);
        }
    }
}
