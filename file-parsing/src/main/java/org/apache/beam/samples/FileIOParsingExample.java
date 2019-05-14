package org.apache.beam.samples;

import static org.apache.beam.sdk.io.Compression.AUTO;

import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.jpeg.JpegDirectory;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raads .jpg files from a directory (it can be in a distributed filesystem). And extracts its width
 * and height and saves them as an Avro object,
 */
public class FileIOParsingExample {
  private static final Logger LOG = LoggerFactory.getLogger(FileIOParsingExample.class);

  /** Specific pipeline options. */
  public interface Options extends PipelineOptions {
    @Description("Input Path")
    String getInput();

    void setInput(String value);

    @Description("Output Path")
    String getOutput();

    void setOutput(String value);
  }

  private static final String SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"Image\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"width\", \"type\": \"int\"},\n"
          + "     {\"name\": \"height\", \"type\": \"int\"}\n"
          + " ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    final String inputPath = options.getInput();
    final String outputPath = options.getOutput();

    //    final String inputPath = "~/Pictures/*.jpg";
    //    final String outputPath = "~/temp/parsing/example-";

    // .withValidation().as(EventsByLocation.Options.class);
    LOG.info(options.toString());
    Pipeline pipeline = Pipeline.create(options);

    // Build a collection of paths to be read
    PCollection<String> paths = pipeline.apply(Create.of(inputPath));

    // Match the paths (globs)
    PCollection<FileIO.ReadableFile> files =
        paths
            .apply("MatchAllReadFiles", FileIO.matchAll())
            .apply("ReadMatchesReadFiles", FileIO.readMatches().withCompression(AUTO));

    // Read via InputStream and parse the files
    PCollection<GenericRecord> genericRecords =
        files
            .apply(
                "ParseFiles",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, GenericRecord>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        FileIO.ReadableFile file = c.element();
                        InputStream inputStream = Channels.newInputStream(file.open());
                        Metadata metadata = ImageMetadataReader.readMetadata(inputStream);
                        Directory jpegDirectory =
                            metadata.getFirstDirectoryOfType(JpegDirectory.class);

                        GenericRecord record = new GenericData.Record(SCHEMA);
                        record.put("name", file.getMetadata().resourceId().getFilename());
                        record.put("width", jpegDirectory.getInt(JpegDirectory.TAG_IMAGE_WIDTH));
                        record.put("height", jpegDirectory.getInt(JpegDirectory.TAG_IMAGE_HEIGHT));
                        c.output(record);
                      }
                    }))
            .setCoder(AvroCoder.of(SCHEMA));

    // Write the output into an Avro file
    genericRecords.apply(AvroIO.writeGenericRecords(SCHEMA).to(outputPath));

    pipeline.run();
  }
}
