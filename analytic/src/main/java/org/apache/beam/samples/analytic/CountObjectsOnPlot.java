package org.apache.beam.samples.analytic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

/**
 * An example of streaming job which reads unbounded data from Kafka.
 * It implements the following pipeline:
 * <pre>
 *     - Consume a message from Kafka queue once it was arrived. Every message contains the coordinates (x,y) of point on the plot.
 *     - Filter out the points that are out of defined region on the plot.
 *     - Count the number of points inside the region during the defined period of time (window size, seconds).
 *     - Write calculated value into text file.
 * </pre>
 *
 * Format of message with coordinates:
 * <pre>
 *     id,x,y
 * </pre>
 */
public class CountObjectsOnPlot {

    static final int WINDOW_SIZE = 10;  // Default window duration in seconds
    static final int COORD_X = 100;  // Default maximum coordinate value (axis X)
    static final int COORD_Y = 100;  // Default maximum coordinate value (axis Y)
    static final String OUTPUT_PATH = "/tmp/beam/cars_report";  // Default output path

    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        @Description("Fixed window duration, in seconds")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Maximum coordinate value (axis X)")
        @Default.Integer(COORD_X)
        Integer getCoordX();
        void setCoordX(Integer value);

        @Description("Maximum coordinate value (axis Y)")
        @Default.Integer(COORD_Y)
        Integer getCoordY();
        void setCoordY(Integer value);

        @Description("Output Path")
        @Default.String(OUTPUT_PATH)
        String getOutput();
        void setOutput(String value);
    }

    private static class FilterObjectsByCoordinates implements SerializableFunction<String, Boolean> {
        private Integer maxCoordX;
        private Integer maxCoordY;

        public FilterObjectsByCoordinates(Integer maxCoordX, Integer maxCoordY) {
            this.maxCoordX = maxCoordX;
            this.maxCoordY = maxCoordY;
        }

        public Boolean apply(String input) {
            String[] split = input.split(",");
            if (split.length < 3) {
                return null;
            }
            Integer coordX = Integer.valueOf(split[1]);
            Integer coordY = Integer.valueOf(split[2]);
            return (coordX >= 0 && coordX < this.maxCoordX
                    && coordY >= 0 && coordY < this.maxCoordY);
        }
    }

    public final static void main(String[] args) throws Exception {
        final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
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
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(options.getWindowSize()))))
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes()
                )
                .apply("FilterValidCoords", Filter.by(
                        new FilterObjectsByCoordinates(options.getCoordX(), options.getCoordY())))
                .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
                .apply(MapElements.via(new SimpleFunction<Long, String>() {
                    public String apply(Long element) {
                        return "{\"" + element + "\"}";
                    }
                }))
                .apply(TextIO.write()
                        .to(options.getOutput())
                        .withWindowedWrites()
                        .withNumShards(1));
        pipeline.run();
    }
}