package org.apache.beam.samples.sql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

/**
 * https://issues.apache.org/jira/browse/BEAM-14099
 */
public class WordCountWithNFilters {
  private static final int N = 100;

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> words = p.apply(TextIO.read().from("file://tmp/input.txt"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))));

    for (int i = 0; i < N; i++) {
      words = words.apply(Filter.by((String word) -> !word.isEmpty()));
    }

    words.apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to("wordcounts"));

    p.run().waitUntilFinish();
  }
}
