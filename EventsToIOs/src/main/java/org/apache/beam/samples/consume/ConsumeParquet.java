package org.apache.beam.samples.consume;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class ConsumeParquet {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeParquet.class);

  private static final Schema SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"ioitavro\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestAvroLine\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"row\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    runReadPipeline(options);
  }

  private static void runReadPipeline(Options options) {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Find files", FileIO.match().filepattern(options.getInput()))
        .apply("Read matched files", FileIO.readMatches())
        .apply("Read parquet files", ParquetIO.readFiles(SCHEMA))
        .apply("Map records to strings", MapElements.into(strings()).via(new GetRecordsFn()));

    pipeline.run();
  }

  private static class GetRecordsFn implements SerializableFunction<GenericRecord, String> {

    @Override
    public String apply(GenericRecord input) {
      String row = String.valueOf(input.get("row"));
      System.out.println("row = " + row);
      return row;
    }
  }

  /** Specific pipeline options. */
  private interface Options extends PipelineOptions {
    @Description("Input Path")
    String getInput();

    void setInput(String value);
  }
}
