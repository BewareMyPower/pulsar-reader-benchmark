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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

public class ReadNextAsync extends AbstractKeyValueReader {

    public ReadNextAsync(PulsarClient client, int queueSize) {
        super(client, queueSize);
    }

    @Override
    public Map<String, Integer> read(String topic) throws Exception {
        try (var reader = client.newReader(Schema.INT32).topic(topic).startMessageId(MessageId.earliest)
                .receiverQueueSize(queueSize)
                .readCompacted(true).create()) {
            final var future = new CompletableFuture<Void>();
            final var map = new HashMap<String, Integer>();
            readToLatest(reader, future, map);
            future.get();
            return map;
        }
    }

    private static void readToLatest(Reader<Integer> reader, CompletableFuture<Void> future, Map<String, Integer> map) {
        reader.hasMessageAvailableAsync().thenCompose(hasMessageAvailable -> {
            if (!hasMessageAvailable) {
                return CompletableFuture.completedFuture(null);
            }
            return reader.readNextAsync();
        }).thenAccept(msg -> {
            if (msg == null) {
                future.complete(null);
                return;
            }
            map.put(msg.getKey(), msg.getValue());
            readToLatest(reader, future, map);
        });
    }
}
