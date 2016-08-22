package ch6.sort.secondary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by hua on 22/08/16.
 */
public class PersonNamePartitioner extends  Partitioner<Person, Text> {

    @Override
    public int getPartition(Person key, Text value, int i) {
        return Math.abs(key.getLastName().hashCode() * 127) % i;
    }
}
