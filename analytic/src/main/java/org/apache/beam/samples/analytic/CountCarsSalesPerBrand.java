package org.apache.beam.samples.analytic;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example of batch pipeline showing how to count number of sales per cars brand.
 * Input file(s) should be a text log in the following format:
 * <pre>
 *     id,brand_name,model_name,sales_number
 * </pre>
 *
 * Example of input log:
 * <pre>
 *     1,Renault,Scenic,3
 *     2,Peugeut,307,2
 *     1,Renault,Megane,4
 *     3,Citroen,c3,5
 *     3,Citroen,c5,3
 * </pre>
 *
 * Example of output result:
 * <pre>
 *     Citroen: 8
 *     Renault: 7
 *     Peugeut: 2
 * </pre>
 *
 */
public class CountCarsSalesPerBrand {

    private static final Logger LOG = LoggerFactory.getLogger(CountCarsSalesPerBrand.class);

    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        @Description("Input Path")
        String getInput();

        void setInput(String value);

        @Description("Output Path")
        String getOutput();

        void setOutput(String value);
    }

    public static final void main(String args[]) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput("/tmp/beam/cars_sales_log.csv");
        }
        if (options.getOutput() == null) {
            options.setOutput("/tmp/beam/cars_sales_report");
        }
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(TextIO.read().from(options.getInput()))
                .apply("ParseAndConvertToKV", MapElements.via(new SimpleFunction<String, KV<String, Integer>>() {
                    @Override
                    public KV<String, Integer> apply(String input) {
                        String[] split = input.split(",");
                        if (split.length < 4) {
                            return null;
                        }
                        String key = split[1];
                        Integer value = Integer.valueOf(split[3]);
                        LOG.debug(String.format("PARSED KV: KEY: |%s| VALUE: |%s|", key, value));
                        return KV.of(key, value);
                    }
                }))
                .apply(GroupByKey.<String, Integer>create())
                .apply("SumUpValuesByKey", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        Integer totalSells = 0;
                        String brand = context.element().getKey();
                        Iterable<Integer> sells = context.element().getValue();
                        for (Integer amount : sells) {
                            totalSells += amount;
                        }
                        context.output(brand + ": " + totalSells);
                    }
                }))
                .apply(TextIO.write().to(options.getOutput()));

        pipeline.run();
    }
}
