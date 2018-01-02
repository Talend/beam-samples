import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

public class TearDown {

    final static int SLEEP_TIME = 5000;

    static class LongTearDownFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            System.out.println(c.element());
            c.output(c.element());
        }

        @FinishBundle
        public void finishBundle() throws InterruptedException {
            runLongMethod("finishBundle()");
        }

        @Teardown
        public void teardown() throws InterruptedException {
            runLongMethod("teardown()");
        }

        private void runLongMethod(String name) throws InterruptedException {
            long beginTs = System.currentTimeMillis();
            long tId = Thread.currentThread().getId();
            System.out.println("Thread #" + tId + ", call " + name);
            Thread.sleep(SLEEP_TIME);
            long endTs = System.currentTimeMillis();
            System.out.println("Thread #" + tId +  ", run for " + (endTs - beginTs) + " ms");
        }
    }

    public static void main(String[] args) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        runPipeline(Pipeline.create(options));
    }

    public static void runPipeline(Pipeline p) {
        System.out.println("Sleep time: " + TearDown.SLEEP_TIME + " ms");

        long tId = Thread.currentThread().getId();
        long beginTs = System.currentTimeMillis();

        p.apply(Create.of("value"))
            .apply(ParDo.of(new LongTearDownFn()));
        p.run().waitUntilFinish();

        long endTs = System.currentTimeMillis();

        System.out.println("Thread #" + tId +  ", run for " + (endTs - beginTs) + " ms");
    }
}
