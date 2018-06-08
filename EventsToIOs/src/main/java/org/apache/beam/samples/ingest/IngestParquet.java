package org.apache.beam.samples.ingest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestParquet {
  private static final Logger LOG = LoggerFactory.getLogger(IngestParquet.class);

  private static final Schema SCHEMA = new Schema.Parser().parse("{\n"
      + " \"namespace\": \"ioitavro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"TestAvroLine\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"row\", \"type\": \"string\"}\n"
      + " ]\n"
      + "}");

  /**
   * Constructs text lines in files used for testing.
   */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  private static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(
          new GenericRecordBuilder(SCHEMA).set("row", c.element()).build()
      );
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(10))
        .apply("Produce text lines", ParDo.of(new DeterministicallyConstructTestTextLineFn()))
        .apply("Produce Avro records", ParDo.of(new DeterministicallyConstructAvroRecordsFn()))
        .setCoder(AvroCoder.of(SCHEMA))
        .apply(
            "Write Parquet files",
            FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(options.getOutput()));

    pipeline.run();
  }

  /**
   * Specific pipeline options.
   */
  private interface Options extends PipelineOptions {
    @Description("Output Path")
    String getOutput();
    void setOutput(String value);
  }
}
