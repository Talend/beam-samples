package org.apache.beam.samples.join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads text data from two different Kafka topics and group the values by key.
 *
 */
public class JoinInStreaming {

  private final static Logger LOG = LoggerFactory.getLogger(JoinInStreaming.class);

  static final String OUTPUT_PATH = "/tmp/kafka_join";
  static final String BOOTSTRAP_SERVERS = "localhost:9092";
  static final String TOPIC_1 = "BEAM_1";
  static final String TOPIC_2 = "BEAM_2";
  public static final int WINDOW_SIZE = 20; // secs

  /**
   * Specific pipeline options.
   */
  private interface Options extends PipelineOptions {

    @Description("Kafka bootstrap servers")
    @Default.String(BOOTSTRAP_SERVERS)
    String getBootstrap();
    void setBootstrap(String value);

    @Description("Output Path")
    @Default.String(OUTPUT_PATH)
    String getOutput();
    void setOutput(String value);

    @Description("Kafka first topic name")
    @Default.String(TOPIC_1)
    String getTopic1();
    void setTopic1(String value);

    @Description("Kafka second topic name")
    @Default.String(TOPIC_2)
    String getTopic2();
    void setTopic2(String value);

    @Description("Fixed window duration, in seconds")
    @Default.Integer(WINDOW_SIZE)
    Integer getWindowSize();
    void setWindowSize(Integer value);
  }

  public static void main(String[] args) throws Exception {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Input from first Kafka topic
    // Format:
    // key=StringValue
    PCollection<KV<String, String>> input1 = pipeline
        .apply(readKafkaTopic(options, options.getTopic1()))
        .apply(ParDo.of(new DoFn<KafkaRecord<Long, String>, KV<String, String>>() {

          @ProcessElement
          public void processElement(ProcessContext processContext) {
            KafkaRecord<Long, String> record = processContext.element();
            String[] split = record.getKV().getValue().split("=");
            processContext.output(KV.of(split[0], split[1]));
          }
        }))
        .apply(Window.<KV<String, String>>into(
            FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
            .withAllowedLateness(Duration.ZERO).discardingFiredPanes());

    // Input from second Kafka topic
    // Format:
    // key=IntegerValue
    PCollection<KV<String, Integer>> input2 = pipeline
        .apply(readKafkaTopic(options, options.getTopic2()))
        .apply(ParDo.of(new DoFn<KafkaRecord<Long, String>, KV<String, Integer>>() {

          @ProcessElement
          public void processElement(ProcessContext processContext) {
            KafkaRecord<Long, String> record = processContext.element();
            String[] split = record.getKV().getValue().split("=");
            processContext.output(KV.of(split[0], Integer.valueOf(split[1])));
          }
        }))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
            .withAllowedLateness(Duration.ZERO).discardingFiredPanes());

    final TupleTag<String> tag1 = new TupleTag<>();
    final TupleTag<Integer> tag2 = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> join = KeyedPCollectionTuple.of(tag1, input1)
        .and(tag2, input2).apply(CoGroupByKey.<String>create());

    PCollection<String> finalResults = join
        .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

          @ProcessElement
          public void processElement(ProcessContext processContext) {
            StringBuilder builder = new StringBuilder();
            KV<String, CoGbkResult> element = processContext.element();
            builder.append(element.getKey()).append(": [");
            Iterable<String> tag1Val = element.getValue().getAll(tag1);
            for (String val : tag1Val) {
              builder.append(val).append(",");
            }
            Iterable<Integer> tag2Val = element.getValue().getAll(tag2);
            for (Integer val : tag2Val) {
              builder.append(val).append(",");
            }
            builder.append("]");
            processContext.output(builder.toString());
          }
        }));

    finalResults.apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext processContext) {
        processContext.output(processContext.element());
      }
    }))
    .apply(TextIO.write().to(options.getOutput()).withWindowedWrites().withNumShards(1));

    pipeline.run().waitUntilFinish();
  }

  private static KafkaIO.Read<Long, String> readKafkaTopic(Options options, String topic1) {
    return KafkaIO.<Long, String>read().withBootstrapServers(options.getBootstrap())
        .withTopic(topic1).withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializer(StringDeserializer.class);
  }
}
