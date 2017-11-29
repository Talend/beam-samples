import org.json.JSONObject;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;

public class SerializationTest implements Serializable {
    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();
    @Test
    public void nonSerilizableTest(){
        PCollection<String> input = pipeline.apply(Create.of("{\"name\":\"one\"}", "{\"name\":\"two\"}", "{\"name\":\"three\"}"));
        //redundant
        input.setCoder(StringUtf8Coder.of());
        PCollection<String> output = input.apply(ParDo.of(new DoFn<String, String>() {

            //even if JSONObject it is not serializable, it passes!
            @ProcessElement public void processElement(ProcessContext context) {
                String element = context.element();
                JSONObject jsonObject = new JSONObject(element);
                String result = new MyFn().apply(jsonObject);
                context.output(result);
            }
        }));
        PAssert.that(output).containsInAnyOrder("one", "two", "three");
        pipeline.run();
    }

    private static class MyFn implements SerializableFunction<JSONObject, String>{

        @Override public String apply(JSONObject jsonObject) {
            return jsonObject.getString("name");
        }
    }
}
