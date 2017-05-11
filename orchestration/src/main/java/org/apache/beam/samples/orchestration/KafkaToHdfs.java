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
                .apply(KafkaIO.<Long, String>read().withBootstrapServers("localhost:9092").withTopic("BEAM").withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class))
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
                        .withFilenamePolicy(new PerWindowFiles("uc5"))
                        .withWindowedWrites()
                        .withNumShards(1));
        pipeline.run();
    }

    public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {

        private final String prefix;

        public PerWindowFiles(String prefix) {
            this.prefix = prefix;
        }

        public String filenamePrefixForWindow(IntervalWindow window) {
            return String.format("%s-%s-%s",
                    prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
        }

        @Override
        public ResourceId windowedFilename(
                ResourceId outputDirectory, WindowedContext context, String extension) {
            IntervalWindow window = (IntervalWindow) context.getWindow();
            String filename = String.format(
                    "%s-%s-of-%s%s",
                    filenamePrefixForWindow(window), context.getShardNumber(), context.getNumShards(),
                    extension);
            return outputDirectory.resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        @Nullable
        @Override
        public ResourceId unwindowedFilename(ResourceId resourceId, Context context, String s) {
            throw new UnsupportedOperationException("Unsupported.");
        }
    }

}
