package com.spark.streaming.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.spark.streaming.htm.HTMNetwork;
import com.spark.streaming.htm.MonitoringRecord;
import org.apache.spark.streaming.rdd.MapWithStateRDDRecord;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization)
public class SparkKryoHTMSerializer<T> extends Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKryoHTMSerializer.class);
    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkKryoHTMSerializer() {
        htmSerializer.registerClass(MapWithStateRDDRecord.class);
        htmSerializer.registerClass(Network.class);
        htmSerializer.registerClass(MonitoringRecord.class);
        htmSerializer.registerClass(HTMNetwork.class);
    }

    @Override
    public void write(Kryo kryo, Output output, T t) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream(512);
             HTMObjectOutput writer = htmSerializer.getObjectOutput(stream)) {
            writer.writeObject(t, t.getClass());
            writer.flush();
            output.writeInt(stream.size());
            stream.writeTo(output);
            LOGGER.info("wrote {} bytes", stream.size());
        } catch (IOException e) {
            LOGGER.error("Can't write kryo object!", e);
            throw new KryoException(e);
        }
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> aClass) {
        // read the serialized data
        byte[] data = new byte[input.readInt()];
        input.readBytes(data);
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data);
             HTMObjectInput reader = htmSerializer.getObjectInput(stream)) {

            T t = (T) reader.readObject(aClass);
            htmSerializer.postDeSerialize(t);
            return t;
        } catch (Exception e) {
            LOGGER.error("Can't read kryo object!", e);
            throw new KryoException(e);
        }
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(MapWithStateRDDRecord.class, new SparkKryoHTMSerializer<>());
        kryo.register(HTMNetwork.class, new SparkKryoHTMSerializer<>());
        kryo.register(Network.class, new SparkKryoHTMSerializer<>());
        kryo.register(MonitoringRecord.class, new SparkKryoHTMSerializer());
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES) {
            kryo.register(c, new SparkKryoHTMSerializer<>());
        }
    }
}