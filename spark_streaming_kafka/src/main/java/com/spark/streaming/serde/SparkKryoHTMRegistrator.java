package com.spark.streaming.serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

import static com.spark.streaming.serde.SparkKryoHTMSerializer.registerSerializers;


public class SparkKryoHTMRegistrator implements KryoRegistrator {

    public SparkKryoHTMRegistrator() {
    }

    @Override
    public void registerClasses(Kryo kryo) {
        // the rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
        // therefore we'll create the seralizers which will use HTMObjectInput/HTMObjectOutput (wrappers on top of fast-serialization)
        // which WILL call the preSerialize/postDeserialize
        registerSerializers(kryo);
    }
}