package org.apache.beam.samples.normalization;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.io.StringReader;

/**
 * Receive XML messages from ActiveMQ, convert as JSON and store into Elasticsearch.
 */
public class JmsToElasticsearch {

    public final static void main(String args[]) throws Exception {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

        pipeline
                .apply(JmsIO.read().withConnectionFactory(connectionFactory).withQueue("BEAM"))
                .apply(ParDo.of(new DoFn<JmsRecord, Biography>() {

                    private JAXBContext jaxbContext;
                    private Unmarshaller unmarshaller;

                    @Setup
                    public void setup() throws Exception {
                        jaxbContext = JAXBContext.newInstance(Biography.class);
                        unmarshaller = jaxbContext.createUnmarshaller();
                    }

                    @ProcessElement
                    public void processElement(ProcessContext processContext) throws Exception {
                        JmsRecord jmsRecord = processContext.element();
                        String payload = jmsRecord.getPayload();
                        Biography biography = (Biography) unmarshaller.unmarshal(new StringReader(payload));
                        processContext.output(biography);
                    }
                }))
                .apply(ParDo.of(new DoFn<Biography, String>() {

                    private ObjectMapper mapper;

                    @Setup
                    public void setup() {
                        mapper = new ObjectMapper();
                    }

                    @ProcessElement
                    public void processElement(ProcessContext processContext) throws Exception {
                        Biography biography = processContext.element();
                        String json = mapper.writeValueAsString(biography);
                        processContext.output(json);
                    }
                }))
                .apply(ElasticsearchIO.write().withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(new String[]{ "http://localhost:9200" }, "beam", "test")));

        pipeline.run();
    }

    @XmlRootElement
    public static class Biography implements Serializable {

        @XmlElement
        public String artist;

        @XmlElement
        public String genre;

        @XmlElement
        public String year;

        @Override
        public String toString() {
            return artist + " [" + genre + "] (" + year + ")";
        }
    }

}
