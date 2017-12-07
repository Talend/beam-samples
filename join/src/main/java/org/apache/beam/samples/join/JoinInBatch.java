package org.apache.beam.samples.join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinInBatch {

    private final static Logger LOG = LoggerFactory.getLogger(JoinInBatch.class);

    public static void main(String[] args) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        //PCollection<KV<String, String>> data1 =  null;
        // window on data1
        // PCollection<KV<String, String>> subData1 = data1.apply(Window.<KV<String, String>>into(new GlobalWindows())
        //        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(3))))
        //        .withAllowedLateness(Duration.standardSeconds(0)));

        //PCollection<KV<String, String>> data2;
        //PCollection<KV<String, String>> data3;

        // transform/format
        //PCollection<KV<Integer, POJO1>> fromData1;
        //PCollection<KV<Integer, POJO2>> fromData2;
        //PCollection<KV<Integer, POJO3>> fromData3;

        // group data1
        //PCollection<KV<Integer, Iterable<POJO1>> groupPojo1 = fromData1.apply(GroupByKey.<Integer, POJO1>create());

        List<KV<String, String>> data1 = new ArrayList<>();
        data1.add(KV.of("key1", "value1"));

        PCollection<KV<String, String>> input1 = pipeline.apply(Create.of(data1));

        List<KV<String, Integer>> data2 = new ArrayList<>();
        data2.add(KV.of("key1", 2));

        PCollection<KV<String,  Integer>> input2 = pipeline.apply(Create.of(data2));

        final TupleTag<String> tag1 = new TupleTag<>();
        final TupleTag<Integer> tag2 = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> join = KeyedPCollectionTuple.of(tag1, input1)
                .and(tag2, input2)
                .apply(CoGroupByKey.<String>create());

        PCollection<String> finalResults = join.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                StringBuilder builder = new StringBuilder();
                KV<String, CoGbkResult> element = processContext.element();
                builder.append(element.getKey()).append(": [");
                Iterable<String> tag1Val = element.getValue().getAll(tag1);
                for (String val : tag1Val) {
                    builder.append(val).append(",");
                }
                Iterable<Integer> tag2Val = element.getValue().getAll(tag2);
                for (Integer val : tag2Val) {
                    builder.append(val).append(",");
                }
                builder.append("]");
                processContext.output(builder.toString());
            }
        }));

        finalResults.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                String element = processContext.element();
                if (!element.equals("key1: [value1,2,]")) {
                    throw new RuntimeException("ERROR");
                }
                LOG.warn(element);
                System.out.println(element);
            }
        }));

        PAssert.that(finalResults).containsInAnyOrder("key1: [value1,2,]");

        pipeline.run();

    }

}
