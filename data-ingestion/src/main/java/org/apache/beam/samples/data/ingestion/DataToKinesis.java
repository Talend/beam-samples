package org.apache.beam.samples.data.ingestion;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.joda.time.Instant;
import software.amazon.kinesis.common.InitialPositionInStream;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;

public class DataToKinesis {
  public static final int NUM_RECORDS = 10;

  /**
   * Specific pipeline options.
   */
  public interface Options extends PipelineOptions, AwsOptions {
    @Description("Kinesis Stream Name")
    String getStream();
    void setStream(String value);
  }

  public static class RandomPartitioner implements KinesisPartitioner<byte[]> {
    @Nonnull
    @Override
    public String getPartitionKey(byte[] record) {
      return UUID.randomUUID().toString();
    }
  }

  private static List<byte[]> prepareData() {
    List<byte[]> data = newArrayList();
    for (int i = 0; i < NUM_RECORDS; i++) {
      data.add(String.valueOf(i).getBytes());
    }
    return data;
  }

  public static void main(String[] args) throws Exception {
    Instant now = Instant.now();

    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    List<byte[]> data = prepareData();

    // Write data into stream
    p.apply(Create.of(data))
        .apply(
            KinesisIO.<byte[]>write()
                    .withStreamName(options.getStream())
                    .withPartitioner(new RandomPartitioner())
                    .withSerializer(r -> r));

    p.run().waitUntilFinish();

    // Read new data from stream that was just written before
    p.apply(
        KinesisIO.read()
            .withStreamName(options.getStream())
            .withMaxNumRecords(data.size())
            // to prevent endless running in case of error
            .withMaxReadTime(Duration.standardMinutes(1))
            .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
            .withInitialTimestampInStream(now))
        .apply(ParDo.of(new DoFn<KinesisRecord, byte[]>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            KinesisRecord record = c.element();
            byte[] data = record.getData().array();
            c.output(data);
          }
        }));

    p.run().waitUntilFinish();
  }
}
