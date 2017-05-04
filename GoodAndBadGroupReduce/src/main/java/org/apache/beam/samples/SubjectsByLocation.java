package org.apache.beam.samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;

/**
 * This class calculates the weight of each subjects in each location. The weigh is equal to the number of events
 * related to this subject in the location
 *
 */

public class SubjectsByLocation {
    private static final Logger LOG = LoggerFactory.getLogger(SubjectsByLocation.class);

    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(GDELTFileFactory.class)
        String getDate();

        void setDate(String value);

        @Description("Input Path")
        String getInput();

        void setInput(String value);

        @Description("Output Path")
        String getOutput();

        void setOutput(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    private static String getCountry(String row) {
        String[] fields = row.split("\\t+");
        if (fields.length > 22) {
            if (fields[21].length() > 2) {
                return fields[21].substring(0, 1);
            }
            return fields[21];
        }
        return "NA";
    }

    private static String getSubject(String row) {
        String[] fields = row.split("\\t+");
        if (fields.length >= 7 && fields[6].length() > 0) {
            return fields[6];
        }
        return "NA";
    }

    private static String getCompositeKey(String row) {
        StringBuilder compositeKey = new StringBuilder();
        //TODO refactor, we tokenize twice
        String country = getCountry(row);
        String subject = getSubject(row);
        if (!"NA".equals(country) && country.length() == 2 && !country.startsWith("-") && !"NA".equals(subject)) {
            compositeKey.append(country).append("_").append(subject);
            return (compositeKey.toString());
        }
        return "NA";
    }

    private static class SubjectsByLocationTransformGood extends PTransform<PCollection<String>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<String> inputCollection) {

            PCollection<String> compositeKeys =
                    inputCollection.apply("ExtractCompositeKey", ParDo.of(new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            c.output(getCompositeKey(c.element()));
                        }
                    })).apply("FilterValidCompositeKeys", Filter.by(new SerializableFunction<String, Boolean>() {
                        public Boolean apply(String input) {
                            return (!input.equals("NA"));
                        }
                    }));
            PCollection<KV<String, Long>> compositesEventsPairs =
                    compositeKeys.apply("GetEventsByCompositeKey", Count.<String>perElement());

            PCollection<String> result = compositesEventsPairs.apply("FormatOutput", MapElements.via(
                    new SimpleFunction<KV<String, Long>, String>() {
                        @Override
                        public String apply(KV<String, Long> kv) {
                            StringBuilder str = new StringBuilder();
                            String[] split = kv.getKey().split("_");
                            String country = split[0];
                            String subject = split[1];
                            Long eventsNb = kv.getValue();
                            str.append(country).append(" ").append(subject).append(" ").append(eventsNb);
                            return str.toString();
                        }
                    }));

            return result;
        }

    }

    private static class subjectsByLocationTransformBad extends PTransform<PCollection<String>, PCollection<String>> {

        private static class Concerns extends HashMap<String, Long> {
            public static Coder getCoder() {
                return MapCoder.of(StringUtf8Coder.of(), VarLongCoder.of());
            }
        }

        @Override
        public PCollection<String> expand(PCollection<String> inputCollection) {

            PCollection<KV<String, String>> countriesSubjectsPairs =
                    inputCollection.apply("ExtractCountrySubjectPairs",
                                          MapElements.via(new SimpleFunction<String, KV<String, String>>() {
                                              @Override
                                              public KV<String, String> apply(String s) {
                                                  return KV.of(getCountry(s), getSubject(s));
                                              }
                                          }))
                            .apply("FilterValidPairs",
                                   Filter.by(new SerializableFunction<KV<String, String>, Boolean>() {
                                       public Boolean apply(KV<String, String> input) {
                                           String country = input.getKey();
                                           String subject = input.getValue();
                                           return (!country.equals("NA") && !subject.equals("NA")
                                                   && !country.startsWith("-")
                                                   && country.length() == 2);
                                       }
                                   }));

            //group subjects by country => bad because it shuffles subject data to group them by country (bandwidth use + slowing pipeline).
            // And if a country has many events in the dataset, a given worker will end up
            // having all subject data in memory for that country. Might lead to an out of memory on the worker.

            PCollection<KV<String, Iterable<String>>> subjectsByCountry =
                    countriesSubjectsPairs.apply("GroupSubjectsByCountry", GroupByKey.<String, String>create());

            PCollection<KV<String, Concerns>> countriesConcernsPairs =
                    subjectsByCountry.apply("CountEventsBySubjetsByCountry",
                                            ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, Concerns>>() {
                                                @ProcessElement
                                                public void processElement(ProcessContext c) {
                                                    KV<String, Iterable<String>> kv = c.element();
                                                    Concerns eventsBySubjects = new Concerns();
                                                    for (String subject : kv.getValue()) {
                                                        Long nbOfEvents = eventsBySubjects.get(subject);
                                                        if (nbOfEvents == null) {
                                                            nbOfEvents = 0L;
                                                        }
                                                        eventsBySubjects.put(subject, ++nbOfEvents);
                                                    }
                                                    String country = kv.getKey();
                                                    c.output(KV.of(country, eventsBySubjects));
                                                }
                                            }));
            countriesConcernsPairs.setCoder(KvCoder.of(StringUtf8Coder.of(), Concerns.getCoder()));
            PCollection<String> result = countriesConcernsPairs.apply("FormatOutput", ParDo.of(
                    new DoFn<KV<String, Concerns>, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            KV<String, Concerns> kv = c.element();
                            StringBuilder str = new StringBuilder();
                            String country = kv.getKey();
                            Concerns concerns = kv.getValue();
                            int i = 0;
                            Set<String> subjects = concerns.keySet();
                            for (String subject : subjects) {
                                str.append(country);
                                str.append(" ");
                                str.append(subject);
                                str.append(" ");
                                Long eventsNb = concerns.get(subject);
                                str.append(eventsNb);
                                if (i < subjects.size() - 1) {
                                    str.append("\n");
                                }
                                i++;
                            }
                            c.output(str.toString());
                        }
                    }));
            return result;

        }

    }

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        if (options.getOutput() == null) {
            options.setOutput("/tmp/gdelt-" + options.getDate());
        }
        LOG.info("Common options: " + options.toString());

        Pipeline goodPipeline = Pipeline.create(options);
        goodPipeline.apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
                .apply("TakeASample", Sample.<String>any(10000))
                .apply("GetSubjectsByLocation", new SubjectsByLocationTransformGood())
                .apply("WriteResults", TextIO.write().to(options.getOutput() + "good/"));
        Instant start = Instant.now();
        goodPipeline.run();
        Instant end = Instant.now();
        long runningTimeForGoodPipeline = end.getMillis() - start.getMillis();

        Pipeline badPipeline = Pipeline.create(options);
        badPipeline.apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
                .apply("TakeASample", Sample.<String>any(10000))
                .apply("GetSubjectsByLocation", new subjectsByLocationTransformBad())
                .apply("WriteResults", TextIO.write().to(options.getOutput() + "bad/"));

        start = Instant.now();
        badPipeline.run();
        end = Instant.now();
        long runningTimeForBadPipeline = end.getMillis() - start.getMillis();

        LOG.info(String.format("Good pipeline runs in %d ms", runningTimeForGoodPipeline));
        LOG.info(String.format("Bad pipeline runs in %d ms", runningTimeForBadPipeline));
        LOG.info(String.format("Bad pipeline (with groupBy) is slower of %d ms",
                               runningTimeForBadPipeline - runningTimeForGoodPipeline));

    }

}
