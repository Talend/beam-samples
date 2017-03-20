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

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatastoreToMongo {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreToMongo.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        @Description("Cloud Datastore Entity Kind")
        String getKind();
        void setKind(String value);

        @Description("Dataset namespace")
        String getNamespace();
        void setNamespace(@Nullable String value);

        @Description("Cloud Datastore Entity Kind")
        String getDatastoreLocalhost();
        void setDatastoreLocalhost(String value);

        @Description("DataStore ProjectId")
        String getDatastoreProjectId();
        void setDatastoreProjectId(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        LOG.info(options.toString());;

//        Transport.getJsonFactory().toString()
        Query.Builder q = Query.newBuilder();
        q.addKindBuilder().setName(options.getKind());
        Query query = q.build();

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("ReadFromDatastore", DatastoreIO.v1().read()
                        .withLocalhost(options.getDatastoreLocalhost())
                        .withProjectId(options.getDatastoreProjectId())
                        .withQuery(query))
                .apply("Trace", MapElements.via(new SimpleFunction<Entity, Entity>(new SerializableFunction<Entity, Entity>() {
                    @Override
                    public Entity apply(Entity input) {
                        System.out.println(input);
                        return input;
                    }
                }) {

                }));
//                .apply("ConvertToEntities", ParDo.of(new CreateEntityFn(options.getNamespace(), options.getKind())))
//                .apply("WriteToDatastore",
//                        DatastoreIO.v1().write()
//                                .withLocalhost(options.getDatastoreLocalhost())
//                                .withProjectId(options.getDatastoreProjectId())
//                );
        pipeline.run();
    }
}
