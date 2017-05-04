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
package org.apache.beam.samples.ingest;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class IngestToKafka {

    static class AddTimestampFn extends DoFn<String, String> {
        private static final Duration RAND_RANGE = Duration.standardHours(24);
        private final Instant minTimestamp;

        AddTimestampFn() {
            this.minTimestamp = new Instant(System.currentTimeMillis());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            // Generate a timestamp that falls somewhere in the past RAND_RANGE hours.
            long randMillis = (long) (Math.random() * RAND_RANGE.getMillis());
//            Instant randomTimestamp = minTimestamp.plus(randMillis);
            Instant randomTimestamp = minTimestamp.minus(randMillis);
            // Concept #2: Set the data element with that timestamp.
            c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(IngestToKafka.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(Options.GDELTFileFactory.class)
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
        String getOutputTopic();
        void setOutputTopic(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
//            .apply(ParDo.of(new AddTimestampFn()))
//            .apply(Trace.Log.<String>print())
            .apply("WriteToKafka", KafkaIO.<Void, String>write()
                .withBootstrapServers(options.getKafkaServer())
                .withTopic(options.getOutputTopic())
//                .withKeyCoder(StringUtf8Coder.of())
                .withValueSerializer(StringSerializer.class)
                .values());
        pipeline.run();
    }
}
