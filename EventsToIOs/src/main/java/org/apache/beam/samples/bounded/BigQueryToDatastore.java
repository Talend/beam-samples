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

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToDatastore {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryToDatastore.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        @Description("GDELT file date")
        @Default.InstanceFactory(Options.GDELTFileFactory.class)
        String getDate();
        void setDate(String value);

//        @Description("Pub/Sub Topic")
//        String getTopic();
//        void setTopic(String value);
//
//        @Description("Bigtable projectId")
//        String getProjectId();
//        void setProjectId(String value);
//
//        @Description("Bigtable instanceId")
//        String getInstanceId();
//        void setInstanceId(String value);
//
//        @Description("Bigtable tableId")
//        String getTableId();
//        void setTableId(String value);

        @Description("Cloud Datastore Entity Kind")
        String getDatastoreHost();
        void setDatastoreHost(String value);

        @Description("Cloud Datastore Entity Kind")
//        @Default.String("shakespeare-demo")
        String getKind();
        void setKind(String value);

        @Description("Dataset namespace")
        String getNamespace();
        void setNamespace(@Nullable String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    /**
     * A helper function to create the ancestor key for all created and queried entities.
     */
    static Key makeAncestorKey(@Nullable String namespace, String kind) {
        Key.Builder keyBuilder = makeKey(kind, "root");
        if (namespace != null) {
            keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
        }
        return keyBuilder.build();
    }

    /**
     * A DoFn that creates entity for every line in Shakespeare.
     */
    static class CreateEntityFn extends DoFn<String, Entity> {
        private final String namespace;
        private final String kind;
        private final Key ancestorKey;

        CreateEntityFn(String namespace, String kind) {
            this.namespace = namespace;
            this.kind = kind;

            // Build the ancestor key for all created entities once, including the namespace.
            ancestorKey = makeAncestorKey(namespace, kind);
        }

        public Entity makeEntity(String content) {
            Entity.Builder entityBuilder = Entity.newBuilder();

            // All created entities have the same ancestor Key.
            Key.Builder keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString());
            // NOTE: Namespace is not inherited between keys created with DatastoreHelper.makeKey, so
            // we must set the namespace on keyBuilder. TODO: Once partitionId inheritance is added,
            // we can simplify this code.
            if (namespace != null) {
                keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
            }

            entityBuilder.setKey(keyBuilder.build());
            entityBuilder.getMutableProperties().put("content", makeValue(content).build());
            return entityBuilder.build();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(makeEntity(c.element()));
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        LOG.info(options.toString());;

//        Transport.getJsonFactory().toString()

        Pipeline pipeline = Pipeline.create(options);
//        PCollection<TableRow> readFromBigQuery = pipeline
//                .apply("ReadFromBigQuery", BigQueryIO.Read.fromQuery("SELECT * FROM Q"))
//                .apply(MapElements.via(new SimpleFunction<TableRow, String>() {
//                    @Override
//                    public String apply(TableRow input) {
//                        return "yes";
//                    }
//                }));

//        Key.Builder keyBuilder = DatastoreHelper.makeKey(...);
//        keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
//        Key key = datastore.allocateId(keyFactory.newKey());

        pipeline
                .apply("ReadFrom", Create.of("bicho"))
                .apply("ConvertToEntities", ParDo.of(new CreateEntityFn(options.getNamespace(), options.getKind())))
                .apply("WriteToDatastore",
                        DatastoreIO.v1().write()
                                .withLocalhost("localhost:8887")
                                .withProjectId("projectId")
                );

//        PCollection<Entity> entities = ...;

// * pipeline.apply(DatastoreIO.v1().write().withProjectId(projectId));

//        TableRow t = null;
//        t.getF()

//            .apply("ConvertToMutations",
//                    MapElements.via(new SerializableFunction<String, KV<ByteString, Iterable<Mutation>>>() {
//                        @Override
//                        public KV<ByteString, Iterable<Mutation>> apply(String input) {
//                            String[] fields = input.split("\\t+");
//                            String key = fields[0];
//                            ByteString rowKey = ByteString.copyFromUtf8(key);
//                            Iterable<Mutation> mutations =
//                                    ImmutableList.of(
//                                            Mutation.newBuilder()
//                                                    .setSetCell(Mutation.SetCell.newBuilder().setValue(ByteString.copyFromUtf8(input)))
//                                                    .build());
//                            return KV.of(rowKey, mutations);
//                        }
//                    }).withOutputType(new TypeDescriptor<KV<ByteString, Iterable<Mutation>>>() {}))
//            .apply("WriteToBigTable",
//                    BigtableIO.write()
//                            .withBigtableOptions(bigtableOptions)
//                            .withTableId(options.getTableId()));
        pipeline.run();
    }
}
