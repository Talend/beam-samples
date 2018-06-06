package org.apache.beam.samples.metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class PollingExample {

  static class CounterFn extends DoFn<Long, Long> {
    private Counter totalCount;

    public CounterFn() {
      totalCount = Metrics.counter("PollingExample", "Total Count");
    }

    @ProcessElement
    public void ProcessElement(ProcessContext c) {
      totalCount.inc();
      c.output(c.element());
    }
  }

  public static void queryMetrics(PipelineResult result) {
    MetricQueryResults metrics = result.metrics().queryMetrics(
        MetricsFilter.builder().addNameFilter(MetricNameFilter.inNamespace("PollingExample")).build());
    Iterable<MetricResult<Long>> counters = metrics.getCounters();
    for (MetricResult<Long> counter : counters) {
      System.out.println(counter.getName().name() + " : " + counter.getAttempted() + " " + Instant.now());
    }
  }

  public static void main(String[] args) {
    // Direct runner does block by default --runner=DirectRunner --blockOnRun=false
    System.out.println("START " + Instant.now());
      PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
          GenerateSequence
              .from(0)
              .to(Long.MAX_VALUE)
            .withRate(10, Duration.millis(100)) //Duration.standardSeconds(2))
        )
        .apply(ParDo.of(new CounterFn()));

    final PipelineResult result = pipeline.run();

    // We create a thread to poll the metrics until waitUntilFinish time
    //TODO: Work on failure scenarios for the threads
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(new Runnable() {
          @Override public void run() {
            queryMetrics(result);
          }
        }, 0, 500, TimeUnit.MILLISECONDS);

//    State state = result.waitUntilFinish(Duration.ZERO);
    State state = result.waitUntilFinish(Duration.standardSeconds(5));
    System.out.println("Shutting down client after waitUntilFinish " + Instant.now());

    scheduledFuture.cancel(true);
    executor.shutdownNow();

    //This is a hack to force the finish
    System.exit(0);
  }
}