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
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.ConnectionFactory;

public class ActiveMQToMongo {

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

        @Description("JMS server")
        @Default.String("tcp://localhost:61616")
        String getJMSServer();
        void setJMSServer(String value);

        @Description("JMS queue")
        @Default.String("gdelt")
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

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        LOG.info(options.toString());

        ConnectionFactory connFactory = new ActiveMQConnectionFactory(options.getJMSServer());
        Pipeline pipeline = Pipeline.create(options);

        // now we connect to the queue and process every event
        PCollection<String> data =
            pipeline
                .apply("ReadFromJms", JmsIO.read()
                    .withConnectionFactory(connFactory)
                    .withQueue(options.getJMSQueue())
                    // hack to make the source unbounded, notice that this require different
                    // processing semantics
//                    .withMaxNumRecords(Long.MAX_VALUE)
                    .withMaxNumRecords(100)
                )
                .apply("ExtractPayload", ParDo.of(new DoFn<JmsRecord, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        c.output(c.element().getPayload());
                    }
                })
        );

        // We filter the events for a given country (IN=India) and send them to their own JMS queue
        final String country = "IN";
        PCollection<String> eventsInIndia =
            data.apply("FilterByCountry", Filter.by(new SerializableFunction<String, Boolean>() {
                public Boolean apply(String row) {
                    return getCountry(row).equals(country);
                }
            }));
        eventsInIndia.apply("WriteToJms", JmsIO.write()
                        .withConnectionFactory(connFactory)
                        .withQueue("india")
        );

        // we count the events per country and register them in Mongo
        PCollection<Document> countByCountry =
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
                .apply("ConvertToJson", MapElements.via(new SimpleFunction<KV<String, Long>, Document>() {
                    public Document apply(KV<String, Long> input) {
                        return Document.parse("{\"" + input.getKey() + "\": " + input.getValue() + "}");
                    }
                }));
        countByCountry
            .apply("WriteToMongo",
                    MongoDbIO.write()
                            .withUri(options.getMongoUri())
                            .withDatabase(options.getMongoDatabase())
                            .withCollection(options.getMongoCollection()));
        pipeline.run();
    }

}
