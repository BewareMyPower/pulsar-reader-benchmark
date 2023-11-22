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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.org.apache.commons.codec.digest.DigestUtils;

public class ConsumerNoAckDemo {

    public static Map<String, Integer> read(String topic) throws IOException {
        try (var client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build()) {
            final var consumer = client.newConsumer(Schema.INT32).topic(topic)
                    .subscriptionName("reader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10))
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionMode(SubscriptionMode.NonDurable).subscribe();
            final var msgIds = consumer.getLastMessageIds();
            final var topicToMsgId = new HashMap<String, MessageId>();
            msgIds.forEach(msgId -> topicToMsgId.put(TopicName.get(msgId.getOwnerTopic()).toString(), msgId));
            final var map = new HashMap<String, Integer>();
            while (true) {
                final var msg = consumer.receive();
                if (!topicToMsgId.containsKey(msg.getTopicName())) {
                    System.err.println("Received message for " + msg.getTopicName() + ", which is not contained by "
                            + topicToMsgId.keySet());
                }
                if (msg.getMessageId().compareTo(topicToMsgId.get(msg.getTopicName())) >= 0) {
                    break;
                }
                map.put(msg.getKey(), msg.getValue());
            }
            return map;
        }
    }
}
