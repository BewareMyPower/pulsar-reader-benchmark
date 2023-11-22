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
import java.util.concurrent.Executors;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class ReadNext extends AbstractKeyValueReader {

    public ReadNext(PulsarClient client, int queueSize) {
        super(client, queueSize);
    }

    @Override
    public Map<String, Integer> read(String topic) throws Exception {
        final var executor = Executors.newSingleThreadExecutor();
        final var result = executor.submit(() -> {
            try {
                final var reader = client.newReader(Schema.INT32).topic(topic)
                        .receiverQueueSize(queueSize)
                        .startMessageId(MessageId.earliest).readCompacted(true).create();
                final var map = new HashMap<String, Integer>();
                while (reader.hasMessageAvailable()) {
                    final var msg = reader.readNext();
                    map.put(msg.getKey(), msg.getValue());
                }
                return map;
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }).get();
        executor.shutdown();
        return result;
    }
}
