package com.spark.streaming;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.spark.streaming.htm.MonitoringRecord;
import com.spark.streaming.serde.SparkKryoHTMRegistrator;
import org.junit.Before;
import org.junit.Test;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.SerialConfig;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class KryoSerDeTests {
    private String file = "./data/file.dat";
    private Kryo kryo;
    private Output output;
    private Input input;
    private MonitoringRecord expected;
    private SparkKryoHTMRegistrator registrator = new SparkKryoHTMRegistrator();

    @Before
    public void init() {
        kryo = new Kryo();
        registrator.registerClasses(kryo);
        expected = new MonitoringRecord(
                "10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(","));
        try {
            output = new Output(new FileOutputStream(file));
            input = new Input(new FileInputStream(file));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(KryoSerDeTests.class.getName())
                    .log(Level.SEVERE, null, ex);
        }
    }


    @Test
    public void kryoRegisterSuccess() {
        assertThat(kryo.getRegistration(Network.class), is(not(nullValue())));
        assertThat(kryo.getRegistration(MonitoringRecord.class), is(not(nullValue())));
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES) {
            assertThat(kryo.getRegistration(c), is(not(nullValue())));
        }
    }

    @Test
    public void withCustomSerializerMonitoringRecordKryoSuccess() {
        //Check only for one object because there a lot of in registration
        kryo.writeObject(output, expected);
        output.close();

        MonitoringRecord actual = kryo.readObject(input, MonitoringRecord.class);
        input.close();
        assertThat(actual, is(expected));

    }


}
