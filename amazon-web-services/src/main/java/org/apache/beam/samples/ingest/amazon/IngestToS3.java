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
package org.apache.beam.samples.ingest.amazon;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestToS3 {

  private static final Logger LOG = LoggerFactory.getLogger(IngestToS3.class);

  /** Specific pipeline options. */
  //  public interface Options extends PipelineOptions {
  public interface Options extends S3Options {

    String GDELT_EVENTS_URL = "s3://gdelt-open-data/events/";

    @Description("GDELT file date")
    @Default.InstanceFactory(Options.GDELTFileFactory.class)
    String getDate();

    void setDate(String value);

    @Description("Input Path")
    String getInput();

    void setInput(String value);

    @Description("Output Path")
    String getOutput();

    void setOutput(String value);

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
      options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.csv");
    }
    LOG.info(options.toString());
    System.out.println(options.toString());

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
        .apply("WriteToFS", TextIO.write().to(options.getOutput()));
    pipeline.run();
  }
}
