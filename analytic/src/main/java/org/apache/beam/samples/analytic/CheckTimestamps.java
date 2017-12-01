package org.apache.beam.samples.analytic;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckTimestamps {
    private static final Logger LOG = LoggerFactory.getLogger(CheckTimestamps.class);

    @DefaultCoder(AvroCoder.class)
    static class WordEntry {
        @Nullable String word;
        @Nullable Long ts;

        public WordEntry() {}

        public WordEntry(String word, Long ts) {
            this.word = word;
            this.ts = ts;
        }

        public String getWord() {
            return this.word;
        }

        public Long getTs() {
            return this.ts;
        }
    }

    public static final void main(String args[]) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
//                .apply(TextIO.read().from("/tmp/foo.in"))

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


                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        LOG.info("LINE: " + context.element() +
                                ", TS: " + context.timestamp().getMillis());
                        context.output(context.element());
                    }
                }))


                .apply(Window.<String>into(
                        FixedWindows.of(Duration.standardSeconds(10))))

                .apply(ParDo.of(new DoFn<String, WordEntry>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(" ")) {
                            if (!word.isEmpty()) {
                                c.output(new WordEntry(word, c.timestamp().getMillis()));
                            }
                        }
                    }
                }))


                .apply(ParDo.of(new DoFn<WordEntry, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        LOG.info("WORD: " + context.element().getWord() +
                                ", TS: " + context.timestamp().getMillis());
                        context.output(String.format("WORD: %s, TS1: %d, TS2: %d",
                                context.element().getWord(), context.element().getTs(), context.timestamp().getMillis()));
                    }
                }))

//                .apply(WithTimestamps.of((WordEntry i) -> new Instant(i.getTs())))
//                .apply(ParDo.of(new DoFn<WordEntry, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext context) {
//                        LOG.info("WORD: " + context.element().getWord() +
//                                ", TS: " + context.timestamp().getMillis());
//                        context.output(context.element().getWord());
//                    }
//                }))
                .apply(TextIO.write().to("/tmp/foo.out")
                        .withWindowedWrites()
                        .withNumShards(1));

        pipeline.run();
    }

}
