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
mvn exec:java -Dexec.mainClass='io.bewaremypower.pulsar.Main' -Dexec.args="100 my-topic-2 10000"
```

It will use `my-topic-2` as the topic name, 100 as the number of messages, 10000 as the receiver queue size.

## An example output

```bash
$ mvn exec:java -Dexec.mainClass='io.bewaremypower.pulsar.Main' -Dexec.args="1000000 my-topic-0 10000" 
0 Listener: 18449ms
0 ConsumerNoAck: 20336ms
0 readNextAsync: 18602ms
0 readNext: 18045ms
1 Listener: 18423ms
1 ConsumerNoAck: 18403ms
1 readNextAsync: 18461ms
1 readNext: 18049ms
2 Listener: 19300ms
2 ConsumerNoAck: 18556ms
2 readNextAsync: 18523ms
2 readNext: 18078ms
3 Listener: 18414ms
3 ConsumerNoAck: 18323ms
3 readNextAsync: 18457ms
3 readNext: 19473ms
4 Listener: 18718ms
4 ConsumerNoAck: 18450ms
4 readNextAsync: 18810ms
4 readNext: 17991ms
---- SUMMARY ----
# Listener
18449, 18423, 19300, 18414, 18718
# ConsumerNoAck
20336, 18403, 18556, 18323, 18450
# readNextAsync
18602, 18461, 18523, 18457, 18810
# readNext
18045, 18049, 18078, 19473, 17991
```

Consumer with a non-durable cursor:
- `Listener`: Use message listener to receive message, no acknowledgment, call `getLastMessageId` once.
- `ConsumerNoAck`: Use `Consumer#receive` to receive message, no acknowledgment, call `getLastMessageId` once.

Reader in a `hasMessageAvailable` and `readNext` loop:
- `readNext`: Call synchronous methods in another thread.
- `readNextAsync`: Call asynchronous methods.
