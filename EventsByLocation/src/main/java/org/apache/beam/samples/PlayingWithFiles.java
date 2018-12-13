package org.apache.beam.samples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class PlayingWithFiles {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);
//    final String filePattern = "/home/ismael/test/*.xml";
      final String filePattern = "file:///home/ismael/test/*.xml";
    p.apply("MatchXml", FileIO.match().filepattern(filePattern))
        .apply(
            MapElements.via(
                new SimpleFunction<MatchResult.Metadata, MatchResult.Metadata>() {
                  public MatchResult.Metadata apply(MatchResult.Metadata metadata) {
                      ResourceId resourceId = metadata.resourceId();
                      System.out.println(resourceId.getScheme() + "://" + resourceId.getCurrentDirectory() + resourceId.getFilename());
                    return metadata;
                  }
                }));
    p.run();
  }
}
