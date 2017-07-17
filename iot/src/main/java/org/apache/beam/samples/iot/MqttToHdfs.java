package org.apache.beam.samples.iot;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

/**
 * Retrieve messages from JMS and write on HDFS.
 */
public class MqttToHdfs {

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    public final static void main(String[] args) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(MqttIO.read().withConnectionConfiguration(MqttIO.ConnectionConfiguration.create("tcp://localhost:1883", "BEAM", "BEAM")).withMaxNumRecords(5))
                .apply(ParDo.of(new DoFn<byte[], String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        byte[] element = processContext.element();
                        processContext.output(new String(element));
                    }
                }))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(TextIO.write()
                .to("hdfs://localhost/uc2")
                .withWindowedWrites()
                .withNumShards(1));
        pipeline.run();
    }
}
