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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableList;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EventToKafkaPipeline {

    /**
     * Specific pipeline options.
     */
    private static interface Options extends PipelineOptions {

        @Description("GDELT file prefix (basically date)")
        String getFilePrefix();
        void setFilePrefix(String value);

    }

    public static void main(String[] args) throws Exception {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        String filePrefix = options.getFilePrefix();
        if (filePrefix == null) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            filePrefix = format.format(new Date());
        }

        pipeline
                .apply(TextIO.Read.named("GDELT Daily File").from("http://data.gdeltproject.org/events/" + filePrefix + ".export.CSV.zip").withCompressionType(TextIO.CompressionType.ZIP))
                .apply(KafkaIO.write().withBootstrapServers("localhost:9092").withTopic("gdelt"));

        pipeline.run();
    }

}
