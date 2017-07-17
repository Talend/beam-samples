package org.apache.beam.samples.orchestration;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

public class KafkaToHdfs {

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    public final static void main(String args[]) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(KafkaIO.<Long, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("BEAM")
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class))
                .apply(ParDo.of(new DoFn<KafkaRecord<Long, String>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        KafkaRecord<Long, String> record = processContext.element();
                        processContext.output(record.getKV().getValue());
                    }
                }))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10)))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes()
                )
                .apply(TextIO.write()
                        .to("hdfs://localhost/uc5")
//                        .to(new PerWindowFiles("hdfs://localhost/uc5"))
                        .withWindowedWrites());
//                        .withNumShards(1));
        pipeline.run();
    }
}
