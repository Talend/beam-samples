# IoT Events Processing

This sample shows Beam usage to process IoT events.

It's composed in two parts:
- a Camel route generated (randomly) events sent into an ActiveMQ JMS broker. In a concrete use case, the Camel route could actually expose a REST service receiving the events, or any other channels.
- a Beam pipeline consumes the events from the ActiveMQ JMS broker and do the processing.