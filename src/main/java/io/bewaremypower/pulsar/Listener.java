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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.shade.org.apache.commons.codec.digest.DigestUtils;

public class Listener extends AbstractKeyValueReader {

    public Listener(PulsarClient client, int queueSize) {
        super(client, queueSize);
    }

    @Override
    public Map<String, Integer> read(String topic) throws Exception {
        final var map = new ConcurrentHashMap<String, Integer>();
        final var msgId = new AtomicReference<MessageId>();
        try (final var consumer = client.newConsumer(Schema.INT32).topic(topic)
                .subscriptionName("reader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messageListener((MessageListener<Integer>) (consumer1, msg) -> {
                    map.put(msg.getKey(), msg.getValue());
                    msgId.set(msg.getMessageId());
                })
                .receiverQueueSize(queueSize)
                .subscriptionMode(SubscriptionMode.NonDurable).subscribe()) {
            final var msgIds = consumer.getLastMessageIds();
            if (msgIds.size() != 1) {
                throw new IllegalStateException("getLastMessageIds returns " + msgIds.size() + " topics");
            }
            final var lastMsgId = msgIds.get(0);
            while (msgId.get() == null || msgId.get().compareTo(lastMsgId) < 0) {
                Thread.sleep(1);
            }
            return map;
        }
    }
}
