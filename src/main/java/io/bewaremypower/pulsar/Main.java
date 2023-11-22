/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.bewaremypower.pulsar;

import java.util.HashMap;
import org.apache.pulsar.client.api.PulsarClient;

public class Main {

    public static void main(String[] args) throws Exception {
        final int numMessages = args.length > 1 ? Integer.parseInt(args[0]) : 10;
        final var topic = args.length > 2 ? args[1] : "my-topic";
        System.out.println("Topic: " + topic + ", numMessages: " + numMessages);
        final var produceResult = PrepareData.run(topic, numMessages);
        System.out.println("Initial data: " + Benchmark.mapToString(produceResult));

        final var benchmark = new Benchmark();
        final var client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        final var readersMap = new HashMap<String, KeyValueReader>();
        readersMap.put("readNextAsync", new ReadNextAsync(client));
        readersMap.put("readNext", new ReadNext(client));
        readersMap.put("ConsumerNoAck", new ConsumerNoAckDemo(client));
        for (int i = 0; i < 5; i++) {
            final int index = i;
            readersMap.forEach((name, reader) -> {
                try {
                    final var elapsed = benchmark.run(name, reader, topic, produceResult);
                    System.out.println(index + " " + name + ": " + elapsed + "ms");
                } catch (Exception e) {
                    System.err.println("Failed to run " + name + ": " + e.getMessage());
                }
            });
        }
        benchmark.print();
        client.close();
    }

}
