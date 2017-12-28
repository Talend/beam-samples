/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.samples.unbounded;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.map.ser.std.StringSerializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToKafka {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        @Description("Kafka Bootstrap Servers")
        @Default.String("localhost:9092")
        String getKafkaServer();
        void setKafkaServer(String value);

        @Description("Kafka Topic Name")
        @Default.String("gdelt")
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Kafka Output Topic Name")
        @Default.String("gdelt-india")
        String getOutputTopic();
        void setOutputTopic(String value);

        @Description("Pipeline duration to wait until finish in seconds")
        @Default.Long(-1)
        Long getDuration();
        void setDuration(Long duration);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    private static String getCountry(String row) {
        String[] fields = row.split("\\t+");
        if (fields.length > 22) {
            if (fields[21].length() > 2) {
                return fields[21].substring(0, 1);
            }
            return fields[21];
        }
        return "NA";
    }


    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        LOG.info(options.toString());
        System.out.println(options.toString());
        Pipeline pipeline = Pipeline.create(options);

        // now we connect to the queue and process every event
        PCollection<String> data =
            pipeline
                .apply("ReadFromKafka", KafkaIO.<String, String>read()
                    .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withBootstrapServers(options.getKafkaServer())
                    .withTopics(Collections.singletonList(options.getInputTopic()))
                    .withoutMetadata()
                )
                .apply("ExtractPayload", Values.<String>create());

        data.apply(ParDo.of(new DoFn<String, String>() {
            private final Counter elementsCounter =
                    Metrics.counter("samples" , "elements");

            @ProcessElement
            public void processElement(ProcessContext c) {
                elementsCounter.inc(1);
                System.out.println(String.format("** element |%s| **", c.element()));
            }
        }));
        // We filter the events for a given country (IN=India) and send them to their own Topic
        final String country = "IN";
        PCollection<String> eventsInIndia =
            data.apply("FilterByCountry", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c){
//                    if (getCountry(c.element()).equals(country)){
                        c.output(c.element());
//                    }

                }
            }));

        PCollection<KV<String,String>> eventsInIndiaKV = eventsInIndia
            .apply("ExtractPayload", ParDo.of(new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of("india", c.element()));
                }
            }));

        eventsInIndiaKV.apply("WriteToKafka",
            KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaServer())
                .withTopic(options.getOutputTopic())
                    .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
                .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class));
        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish(Duration.standardSeconds(options.getDuration()));
    }
}
