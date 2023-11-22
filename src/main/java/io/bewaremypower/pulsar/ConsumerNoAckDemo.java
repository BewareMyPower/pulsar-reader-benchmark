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
import java.util.UUID;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.shade.org.apache.commons.codec.digest.DigestUtils;

public class ConsumerNoAckDemo implements KeyValueReader {

    private final PulsarClient client;

    public ConsumerNoAckDemo(PulsarClient client) {
        this.client = client;
    }

    @Override
    public Map<String, Integer> read(String topic) throws Exception {
        try (final var consumer = client.newConsumer(Schema.INT32).topic(topic)
                .subscriptionName("reader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionMode(SubscriptionMode.NonDurable).subscribe()) {
            final var msgIds = consumer.getLastMessageIds();
            if (msgIds.size() > 1) {
                throw new IllegalStateException("getLastMessageIds returns " + msgIds.size() + " topics");
            }
            final var lastMsgId = msgIds.get(0);
            final var map = new HashMap<String, Integer>();
            while (true) {
                final var msg = consumer.receive();
                map.put(msg.getKey(), msg.getValue());
                if (msg.getMessageId().compareTo(lastMsgId) >= 0) {
                    break;
                }
            }
            return map;
        }
    }
}
