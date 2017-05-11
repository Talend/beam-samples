package org.apache.beam.samples.orchestration;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class JmsToKafka {

    public final static void main(String args[]) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Pipeline pipeline = Pipeline.create();
        pipeline
                .apply(JmsIO.read().withConnectionFactory(connectionFactory).withQueue("BEAM"))
                .apply(MapElements.via(new SimpleFunction<JmsRecord, KV<Long, String>>() {
                                           public KV<Long, String> apply(JmsRecord input) {
                                               KV kv = KV.of(System.currentTimeMillis(), input.getPayload());
                                               return kv;
                                           }
                                       }
                ))
                .apply(KafkaIO.<Long, String>write().withBootstrapServers("localhost:9092").withTopic("BEAM").withKeySerializer(LongSerializer.class).withValueSerializer(StringSerializer.class));
        pipeline.run();
    }

}
