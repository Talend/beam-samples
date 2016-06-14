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

        @Description("GDELT file base location")
        @Default.String("http://data.gdeltproject.org/events/")
        String getBaseLocation();
        void setBaseLocation(String value);

        @Description("GDELT file")
        @Default.InstanceFactory(GDELTFileFactory.class)
        String getFile();
        void setFile(String value);

        public static class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date()) + ".export.CSV.zip";
            }
        }

    }

    public static void main(String[] args) throws Exception {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        // String file = options.getFile();
        String file = "20160529.export.CSV";
        // String baseLocation = options.getBaseLocation();
        String baseLocation = "/home/jbonofre/";

        pipeline
                .apply(TextIO.Read.named("GDELT Daily File").from(baseLocation + file))
                .apply(ParDo.of(new DoFn<String, String>() {
                    public void processElement(ProcessContext c) {
                        for (String field : c.element().split("\\s+")) {
                            c.output(field);
                        }
                        c.output(Long.toString(System.currentTimeMillis()));
                    }
                }))
                // send the PCollection to the target sink
                .apply(TextIO.Write.named("GDELT").to("/home/jbonofre/test.csv"));
                //.apply(KafkaIO.write().withBootstrapServers("localhost:9092").withTopic("gdelt"));

        pipeline.run();
    }

}
