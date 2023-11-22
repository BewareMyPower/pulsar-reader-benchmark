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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public class PrepareData {

    public static Map<String, Integer> run(PulsarClient client, String topic, int numMessages)
            throws Exception {
        try (var producer = client.newProducer(Schema.INT32).topic(topic)
                .enableBatching(false).create()) {
            final String[] keys = { "A", "B", "C", "D" };
            final var map = new HashMap<String, Integer>();
            final var latch = new CountDownLatch(numMessages);
            for (int i = 0; i < numMessages; i++) {
                final var key = keys[i % keys.length];
                producer.newMessage().key(key).value(i).sendAsync().thenAccept(__ -> latch.countDown());
                map.put(key, i);
            }
            latch.await();
            return map;
        }
    }
}
