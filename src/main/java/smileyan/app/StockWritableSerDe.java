package smileyan.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Created by hua on 28/06/16.
 */
public class StockWritableSerDe  extends AbstractSerDe {

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return null;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return null;
    }

    @Override
    public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties) throws SerDeException {
        super.initialize(configuration, tableProperties, partitionProperties);
    }

    @Override
    public void initialize(@Nullable Configuration configuration, Properties properties) throws SerDeException {

    }
}
