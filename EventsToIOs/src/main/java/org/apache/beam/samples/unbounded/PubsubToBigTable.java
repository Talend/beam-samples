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

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PubsubToBigTable {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubToBigTable.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        @Description("GDELT file date")
        @Default.InstanceFactory(Options.GDELTFileFactory.class)
        String getDate();
        void setDate(String value);

        @Description("Pub/Sub Topic")
        String getTopic();
        void setTopic(String value);

        @Description("Bigtable projectId")
        String getProjectId();
        void setProjectId(String value);

        @Description("Bigtable instanceId")
        String getInstanceId();
        void setInstanceId(String value);

        @Description("Bigtable tableId")
        String getTableId();
        void setTableId(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        LOG.info(options.toString());

        BigtableOptions bigtableOptions = new BigtableOptions.Builder()
                .setProjectId(options.getProjectId())
                .setInstanceId(options.getInstanceId())
                .build();

        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply("ReadFromTopic", PubsubIO.<String>read().topic(options.getTopic()))
            .apply("ConvertToMutations",
                    MapElements.via(new SerializableFunction<String, KV<ByteString, Iterable<Mutation>>>() {
                        @Override
                        public KV<ByteString, Iterable<Mutation>> apply(String input) {
                            String[] fields = input.split("\\t+");
                            String key = fields[0];
                            ByteString rowKey = ByteString.copyFromUtf8(key);
                            Iterable<Mutation> mutations =
                                    Collections.singletonList(
                                            Mutation.newBuilder()
                                                    .setSetCell(Mutation.SetCell.newBuilder().setValue(ByteString.copyFromUtf8(input)))
                                                    .build());
                            return KV.of(rowKey, mutations);
                        }
                    }).withOutputType(new TypeDescriptor<KV<ByteString, Iterable<Mutation>>>() {}))
            .apply("WriteToBigTable",
                    BigtableIO.write()
                            .withBigtableOptions(bigtableOptions)
                            .withTableId(options.getTableId()));
        pipeline.run();
    }
}
