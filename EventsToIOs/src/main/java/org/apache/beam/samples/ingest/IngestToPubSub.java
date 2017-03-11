package org.apache.beam.samples.ingest;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;


public class IngestToPubSub {
    private static final Logger LOG = LoggerFactory.getLogger(IngestToPubSub.class);

    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(Options.GDELTFileFactory.class)
        String getDate();
        void setDate(String value);

        @Description("Input Path")
        String getInput();
        void setInput(String value);

        @Description("Pub/Sub Topic")
        @Default.String("gdelt")
        String getPubsubTopic();
        void setPubsubTopic(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("ReadFromGDELTFile", TextIO.Read.from(options.getInput()))
                .apply("WriteToJMS", PubsubIO.<String>write().topic(options.getPubsubTopic()));
        pipeline.run();

    }
}
