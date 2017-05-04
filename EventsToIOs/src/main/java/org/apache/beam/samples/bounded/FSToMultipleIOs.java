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
package org.apache.beam.samples.bounded;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.samples.unbounded.KafkaToCassandra;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.ConnectionFactory;

public class FSToMultipleIOs {

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

        @Description("Output Path")
        String getOutput();
        void setOutput(String value);

        @Description("JMS server")
        @Default.String("tcp://localhost:61616")
        String getJMSServer();
        void setJMSServer(String value);

        @Description("JMS queue")
        @Default.String("India")
        String getJMSQueue();
        void setJMSQueue(String value);

        @Description("Mongo Uri")
        @Default.String("mongodb://localhost:27017")
        String getMongoUri();
        void setMongoUri(String value);

        @Description("Mongo Database")
        @Default.String("gdelt")
        String getMongoDatabase();
        void setMongoDatabase(String value);

        @Description("Mongo Collection")
        @Default.String("countbylocation")
        String getMongoCollection();
        void setMongoCollection(String value);

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

    public static PCollection<String> filterByCountry(PCollection<String> data, final String country) {
        return data.apply("FilterByCountry", Filter.by(new SerializableFunction<String, Boolean>() {
            public Boolean apply(String row) {
                return getCountry(row).equals(country);
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        LOG.info(options.toString());

        ConnectionFactory connFactory = new ActiveMQConnectionFactory();
        Pipeline pipeline = Pipeline.create(options);

        // now we connect to the queue and process every event
//        PCollection<String> data =
//            pipeline
//                .apply("ReadFromJms", JmsIO.read()
//                                .withConnectionFactory(connFactory)
//                                .withQueue("gdelt")
////                .withMaxNumRecords(1000)
////                    .withMaxNumRecords(Long.MAX_VALUE)
//                )
//                .apply("ExtractPayload", ParDo.of(new DoFn<JmsRecord, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) throws Exception {
//                        c.output(c.element().getPayload());
//                    }
//                })
//        );


        PCollection<String> data = pipeline.apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()));
        // We filter the events for a given country (IN=India) and send them to their own JMS queue
        PCollection<String> eventsInIndia = filterByCountry(data, "IN");
//        eventsInIndia.apply("WriteToJms", JmsIO.write()
//                        .withConnectionFactory(connFactory)
//                        .withQueue(options.getJMSQueue()));
        eventsInIndia.apply("WriteEventsInIndia", TextIO.write().to(options.getOutput() + "india"));

//        // we count the events per country and register them in Mongo
//        PCollection<String> countByCountry =
//            data
//                .apply("ExtractLocation", ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        c.output(getCountry(c.element()));
//                    }
//                }))
//                .apply("FilterValidLocations", Filter.by(new SerializableFunction<String, Boolean>() {
//                    public Boolean apply(String input) {
//                        return (!input.equals("NA") && !input.startsWith("-") && input.length() == 2);
//                    }
//                }))
//                .apply("CountByLocation", Count.<String>perElement())
//                .apply("Top", Top.of(10, new SerializableComparator<KV<String, Long>>() {
//                        @Override
//                        public int compare(KV<String, Long> o1, KV<String, Long> o2) {
//                            return Long.compare(o1.getValue(), o2.getValue());
//                        }
//                    }).withoutDefaults())
////                .apply("Flat", Flatten.<KV<String, Long>>iterables()) // ?
//                .apply("fsfd", MapElements.via(new SimpleFunction<List<KV<String, Long>>, String>() {
//                    @Override
//                    public String apply(List<KV<String, Long>> kvs) {
//                        String x = "";
//                        for (KV<String, Long> kv : kvs) {
//                            x += "{\"" + kv.getKey() + "\": " + kv.getValue() + "}" + "\r\n";
//                        }
//                        return x;
//                    }
//                }));
////                .apply("ConvertToJson", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
////                    public String apply(KV<String, Long> input) {
////                        return "{\"" + input.getKey() + "\": " + input.getValue() + "}";
////                    }
////                }));
//
//        countByCountry.apply("WriteCountByCountry", TextIO.Write.to(options.getOutput() + "count")); ///"/tmp/beam/beam-samples/count"

//            .apply("WriteToMongo",
//                    MongoDbIO.write()
//                            .withUri(options.getMongoUri())
//                            .withDatabase(options.getMongoDatabase())
//                            .withCollection(options.getMongoCollection()));

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

        long startTime = new Date().getTime();
        pipeline.run().waitUntilFinish();
        long endTime = new Date().getTime();
        System.out.println("Pipeline executed by " + options.getRunner().getSimpleName() + " took: " + (endTime - startTime) + " ms.");
    }
}
