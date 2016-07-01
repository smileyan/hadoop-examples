package ch3.seqfile.protobuf;

import com.google.protobuf.MessageLite;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by hua on 29/06/16.
 */
public class ProtobufSerialization extends Configured implements Serialization<MessageLite> {

    public Deserializer<MessageLite> getDeserializer(Class<MessageLite> aClass) {
        return new ProtobufDeserializer(getConf(), aClass);
    }

    public Serializer<MessageLite> getSerializer(Class<MessageLite> aClass) {
        return new ProtobufSerializer();
    }

    public boolean accept(Class<?> c) {
        return MessageLite.class.isAssignableFrom(c);
    }

    public static void register(Configuration conf) {
        String[] serializations = conf.getStrings("io.serializations");
        if (ArrayUtils.isEmpty(serializations)) {
            serializations = new String[] {
                    WritableSerialization.class.getName(),
                    AvroSpecificSerialization.class.getName(),
                    AvroReflectSerialization.class.getName()
            };
        }
        serializations = ArrayUtils.add(serializations, ProtobufSerialization.class.getName());
        conf.setStrings("io.serializations", serializations);
    }

    static class ProtobufDeserializer extends Configured implements Deserializer<MessageLite> {
        private Class<? extends MessageLite> protobufClass;

        private InputStream in;

        public ProtobufDeserializer(Configuration conf, Class<? extends MessageLite> c) {
            setConf(conf);
            this.protobufClass = c;
        }

        public void open(InputStream inputStream) throws IOException {
            this.in = inputStream;
        }

        public void close() throws IOException {
            IOUtils.closeStream(in);
        }

        public MessageLite deserialize(MessageLite w) throws IOException {
            MessageLite.Builder builder;

            if (w == null) {
                builder = newBuilder();
            } else {
                builder = w.newBuilderForType();
            }

            if (builder.mergeDelimitedFrom(in)) {
                return builder.build();
            }
            return null;
        }

        /**
         * @since 1.6+
         */
        public MessageLite.Builder newBuilder() throws IOException {
            try {
                return (MessageLite.Builder) MethodUtils.invokeExactStaticMethod(protobufClass, "newBuilder");
            } catch (IllegalAccessException illAccE) {
                throw new IOException(illAccE);
            } catch (NoSuchMethodException noMethod) {
                throw new IOException("no such method exception", noMethod);
            } catch (InvocationTargetException inT) {
                throw new IOException("", inT);
            }

        }
    }

    static class ProtobufSerializer extends Configured implements Serializer<MessageLite> {

        OutputStream out;

        public void open(OutputStream outputStream) throws IOException {
            this.out = outputStream;
        }

        public void close() throws IOException {
            IOUtils.closeStream(out);
        }

        public void serialize(MessageLite messageLite) throws IOException {
            messageLite.writeDelimitedTo(out);
        }
    }
}
