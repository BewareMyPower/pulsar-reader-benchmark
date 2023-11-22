# pulsar-reader-benchmark

Benchmark for different ways of using a Pulsar reader to read messages to latest.

Run a Pulsar standalone first. Then build and run tests:

```bash
mvn clean package -DskipTests
mvn exec:java -Dexec.mainClass='io.bewaremypower.pulsar.Main'
```

By default, it sends 10 messages to topic `my-topic` and consumes them.

To customize the number of messages, add the command line argument like:

```bash
mvn exec:java -Dexec.mainClass='io.bewaremypower.pulsar.Main' -Dexec.args="100 my-topic-2"
```

It will use `my-topic-2` as the topic name and 100 as the number of messages.
