package org.apache.beam.samples.iot;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

/**
 * Retrieve messages from JMS and write on HDFS.
 */
public class JmsToHdfs {

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    public final static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(JmsIO.read().withConnectionFactory(connectionFactory).withQueue("BEAM"))
                .apply(ParDo.of(new DoFn<JmsRecord, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        JmsRecord element = processContext.element();
                        processContext.output(element.getPayload());
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
