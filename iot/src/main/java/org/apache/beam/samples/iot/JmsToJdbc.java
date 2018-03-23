package org.apache.beam.samples.iot;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.derby.jdbc.ClientDataSource;
import org.joda.time.Duration;

/**
 * Retrieve messages from JMS and store on HDFS.
 */
public class JmsToJdbc {

    public final static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

        ClientDataSource dataSource = new ClientDataSource();
        dataSource.setPortNumber(1527);
        dataSource.setServerName("localhost");
        dataSource.setDatabaseName("test");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(JmsIO.read().withConnectionFactory(connectionFactory).withQueue("BEAM"))
                .apply(ParDo.of(new DoFn<JmsRecord, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        JmsRecord element = processContext.element();
                        processContext.output(element.getPayload());
                    }
                }))
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30)))
                        .triggering(AfterWatermark.pastEndOfWindow())
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply(JdbcIO.<String>write().withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(dataSource))
                        .withStatement("insert into test values(?)")
                        .withPreparedStatementSetter((element, statement) -> {
                            statement.setString(1, element);
                        }));
        pipeline.run();
    }

}
