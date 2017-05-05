package org.apache.beam.samples.data.ingestion;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.sql.ResultSet;
public class JdbcToHdfs {

    public final static void main(String[] args) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(JdbcIO.<KV<String, String>>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.apache.derby.jdbc.ClientDriver", "jdbc:derby://localhost:1527/beam"))
                        .withQuery("select * from artist")
                        .withRowMapper(new JdbcIO.RowMapper<KV<String, String>>() {
                            @Override
                            public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
                                KV<String, String> kv = KV.of(resultSet.getString("label"), resultSet.getString("name"));
                                return kv;
                            }
                        })
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
                .apply(GroupByKey.<String, String>create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        KV<String, Iterable<String>> element = processContext.element();
                        processContext.output(element.getKey() + ":" + element.getValue());
                    }
                }))
                .apply(TextIO.write().to("hdfs://localhost/uc1"));
        pipeline.run();
    }

}
