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
package org.apache.beam.samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EventToKafkaPipeline {

    /**
     * Specific pipeline options.
     */
    private static interface Options extends PipelineOptions {
        final String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

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

        public static class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        String inputPath = options.getInput();
        if (inputPath == null) {
            inputPath = Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip";
        }

        String outputPath = options.getOutput();
        if (outputPath == null) {
            outputPath = "/tmp/gdelt-" + options.getDate();
        }

        pipeline
                .apply(TextIO.Read.named("GDELT Daily File").from(inputPath))
                .apply(ParDo.of(new DoFn<String, String>() {
                    public void processElement(ProcessContext c) {
                        for (String field : c.element().split("\\s+")) {
                            c.output(field);
                        }
                        c.output(Long.toString(System.currentTimeMillis()));
                    }
                }))
                // send the PCollection to the target sink
                .apply(TextIO.Write.named("GDELT").to(outputPath));
                //.apply(KafkaIO.write().withBootstrapServers("localhost:9092").withTopic("gdelt"));

        pipeline.run();
    }

}
