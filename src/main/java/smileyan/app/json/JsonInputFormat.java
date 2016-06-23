package smileyan.app.json;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by hua on 23/06/16.
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, MapWritable> {

    @Override
    public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new JsonRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return super.isSplitable(context, filename);
    }

    public static class JsonRecordReader extends RecordReader<LongWritable, MapWritable> {

        private static final Logger LOG = LoggerFactory.getLogger(JsonRecordReader.class);

        private final LineRecordReader reader = new LineRecordReader();
        private final MapWritable value = new MapWritable();
        private final JSONParser jsonParser = new JSONParser();

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (reader.nextKeyValue()) {
                value.clear();
                if (toJson(jsonParser, reader.getCurrentValue(), value)) {
                    return true;
                }
            }
            return false;
        }

        public static boolean toJson(JSONParser parser, Text line, MapWritable value) {
            try {
                JSONObject jsonObj = (JSONObject) parser.parse(line.toString());
                for (Object key : jsonObj.keySet()) {
                    Text mapKey = new Text(key.toString());
                    Text mapValue = new Text();

                    if (jsonObj.get(key) != null) {
                        mapValue.set(jsonObj.get(key).toString());
                    }
                    value.put(mapKey, mapValue);
                }
                return true;
            } catch (ParseException e) {
                LOG.warn("Could not json-decode string: " + line, e);
                return false;
            } catch (NumberFormatException e) {
                LOG.warn("Could not parse field into number: " + line, e);
                return false;
            }
        }

        @Override
        public MapWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return reader.getCurrentKey();
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            reader.initialize(inputSplit, taskAttemptContext);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }
    }
}
