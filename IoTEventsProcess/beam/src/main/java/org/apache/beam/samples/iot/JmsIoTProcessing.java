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
package org.apache.beam.samples.iot;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import javax.jms.ConnectionFactory;

public class JmsIoTProcessing {

    public static void main(String[] args) throws Exception {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

        pipeline.apply(JmsIO.read().withConnectionFactory(connectionFactory).withQueue("iot"))
            .apply(ParDo.of(new DoFn<JmsRecord, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    System.out.println(c.element().getPayload());
                    c.output(c.element().getPayload());
                }
            }));

        pipeline.run();
    }

}
