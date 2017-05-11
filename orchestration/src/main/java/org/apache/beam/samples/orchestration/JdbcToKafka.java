package org.apache.beam.samples.orchestration;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.ResultSet;

/**
 * Read content of a data and send to a Kafka topic.
 */
public class JdbcToKafka {

    public static final void main(String args[]) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(JdbcIO.<KV<Long, String>>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.apache.derby.jdbc.ClientDriver", "jdbc:derby://localhost:1527/beam"))
                        .withQuery("select * from artist")
                        .withRowMapper(new JdbcIO.RowMapper<KV<Long, String>>() {
                            @Override
                            public KV<Long, String> mapRow(ResultSet resultSet) throws Exception {
                                KV<Long, String> kv = KV.of(System.currentTimeMillis(), resultSet.getString("name") + "," + resultSet.getString("label"));
                                return kv;
                            }
                        })
                        .withCoder(KvCoder.of(SerializableCoder.of(Long.class), StringUtf8Coder.of())))
                .apply(KafkaIO.<Long, String>write().withBootstrapServers("localhost:9092").withTopic("BEAM").withKeySerializer(LongSerializer.class).withValueSerializer(StringSerializer.class));
        pipeline.run();
    }

}
