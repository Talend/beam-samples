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

import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class KafkaToCassandra {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaToCassandra.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(GDELTFileFactory.class)
        String getDate();
        void setDate(String value);

        @Description("Input Path")
        String getInput();
        void setInput(String value);

        @Description("Kafka Bootstrap Servers")
        @Default.String("localhost:9092")
        String getKafkaServer();
        void setKafkaServer(String value);

        @Description("Kafka Topic Name")
        @Default.String("gdelt")
        String getKafkaTopic();
        void setKafkaTopic(String value);

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
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);

        // now we connect to the queue and process every event
        PCollection<String> data = pipeline
            .apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaServer())
                .withTopics(Collections.singletonList(options.getKafkaTopic()))
                .withoutMetadata()
            )
            .apply("ExtractPayload", Values.<String>create());

        // We filter the events for a given country (IN=India) and send them to their own Topic
        final String country = "IN";
        PCollection<String> eventsInIndia =
            data.apply("FilterByCountry", Filter.by(new SerializableFunction<String, Boolean>() {
                public Boolean apply(String row) {
                    return getCountry(row).equals(country);
                }
            }));

        PCollection<KV<String,String>> eventsInIndiaKV = eventsInIndia
            .apply("ExtractPayload", ParDo.of(new DoFn<String, KV<String, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    c.output(KV.of("india", c.element()));
                }
            }));

        //TODO: Test write
        eventsInIndiaKV.apply("WriteToKafka", KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaServer())
                .withTopic("india")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class));


        // TODO: fix error on grouping without windows
        // we count the events per country and register them in Mongo
        PCollection<String> countByCountry =
            data
                .apply("ExtractLocation", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(getCountry(c.element()));
                    }
                }))
                .apply("FilterValidLocations", Filter.by(new SerializableFunction<String, Boolean>() {
                    public Boolean apply(String input) {
                        return (!input.equals("NA") && !input.startsWith("-") && input.length() == 2);
                    }
                }))
                .apply("CountByLocation", Count.<String>perElement())
                .apply("ConvertToJson", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    public String apply(KV<String, Long> input) {
                        return "{\"" + input.getKey() + "\": " + input.getValue() + "}";
                    }
                }));

        //TODO Finish once CassandraIO is merged
//        countByCountry
//            .apply("ToCassandraRow", ParDo.of(new DoFn<String, CassandraRow>() {
//                @ProcessElement
//                public void processElement(ProcessContext c) {
//                    CassandraRow row = new CassandraRow();
//                    row.add("name", CassandraColumnDefinition.Type.TEXT, c.element());
//                    c.output(row);
//                }
//            }))
//            .apply("WriteToCassandra",
//                    CassandraIO.write()
//                        .withHosts(new String[] {"localhost"})
//                        .withKeyspace("gdelt")
////                        .withTable("percountry")
////                        .withConfig(new HashMap<String, String>())
////                        .withColumns("ab")
//            );

        pipeline.run();
    }

}
