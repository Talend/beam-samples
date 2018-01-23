package org.apache.beam.samples.data.ingestion;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class DataToKinesis {
  public static final int NUM_RECORDS = 10;

  /**
   * Specific pipeline options.
   */
  private interface Options extends PipelineOptions {
    @Description("AWS Access Key")
    String getAccessKey();
    void setAccessKey(String value);

    @Description("AWS Secret Key")
    String getSecretKey();
    void setSecretKey(String value);

    @Description("AWS Region Name")
    String getRegionName();
    void setRegionName(String value);

    @Description("Kinesis Stream Name")
    String getStream();
    void setStream(String value);

    @Description("Partition Key")
    @Default.String("pkey")
    String getPartitionKey();
    void setPartitionKey(String value);
  }

  private static List<byte[]> prepareData() {
    List<byte[]> data = newArrayList();
    for (int i = 0; i < NUM_RECORDS; i++) {
      data.add(String.valueOf(i).getBytes());
    }
    return data;
  }

  public final static void main(String[] args) throws Exception {
    Instant now = Instant.now();

    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    List<byte[]> data = prepareData();

    // Write data into stream
    p.apply(Create.of(data))
        .apply(
            KinesisIO.write()
                .withStreamName(options.getStream())
                .withPartitionKey(options.getPartitionKey())
                .withAWSClientsProvider(
                    options.getAccessKey(),
                    options.getSecretKey(),
                    Regions.fromName(options.getRegionName())));
    p.run().waitUntilFinish();


    // Read new data from stream that was just written before
    PCollection<byte[]> output =
        p.apply(
            KinesisIO.read()
                .withStreamName(options.getStream())
                .withAWSClientsProvider(
                    options.getAccessKey(),
                    options.getSecretKey(),
                    Regions.fromName(options.getRegionName()))
                .withMaxNumRecords(data.size())
                // to prevent endless running in case of error
                .withMaxReadTime(Duration.standardMinutes(1))
                .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
                .withInitialTimestampInStream(now)
        )
            .apply(
                ParDo.of(
                    new DoFn<KinesisRecord, byte[]>() {

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
