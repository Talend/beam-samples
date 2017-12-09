package org.apache.beam.samples.orchestration;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class KafkaToHdfs {
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    static final String OUTPUT_PATH = "/tmp/kafka2hdfs";  // Default output path
    static final String BOOTSTRAP_SERVERS = "localhost:9092";  // Default bootstrap kafka servers
    static final String TOPIC = "BEAM";  // Default kafka topic name

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

        @Description("Kafka topic name")
        @Default.String(TOPIC)
        String getTopic();

        void setTopic(String value);
    }

    public final static void main(String args[]) throws Exception {
        final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(KafkaIO.<Long, String>read()
                        .withBootstrapServers(options.getBootstrap())
                        .withTopic(options.getTopic())
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata())
                .apply(Values.<String>create())
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10)))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes()
                )
                .apply(TextIO.write()
                        .to(options.getOutput())
                        .withWindowedWrites()
                        .withNumShards(1));
        pipeline.run().waitUntilFinish();
    }
}
