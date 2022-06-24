/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.test.HazelcastTestSupport;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.test.util.MQAdmin;

import java.nio.charset.StandardCharsets;

public class RocketmqTestSupport {
    protected DefaultMQProducer producer;
    protected String namesrvAddr;
    protected String clusterName;

    public RocketmqTestSupport(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        ClusterInfo cluster = MQAdmin.getCluster(namesrvAddr);
        this.clusterName = cluster.getClusterAddrTable().keySet().iterator().next();
    }

    public void createTopic(String topicId, int queueNum) {
        MQAdmin.createTopic(namesrvAddr, clusterName, topicId, queueNum);
    }

    public SendResult produce(String topic, Integer key, String value) {
        return produce(topic, String.valueOf(key), value);
    }

    public SendResult produce(String topic, String key, String value) {
        Message message = new Message(topic, value.getBytes(StandardCharsets.UTF_8));
        message.setKeys(String.valueOf(key));
        try {
            return getProducer().send(message);
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private DefaultMQProducer getProducer() {
        if (producer == null) {
            producer = new DefaultMQProducer();
            producer.setNamesrvAddr(namesrvAddr);
            producer.setProducerGroup(HazelcastTestSupport.randomString());
            try {
                producer.start();
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
        return producer;
    }

    public void shutdownKafkaCluster() {
        if (producer != null) {
            producer.shutdown();
        }
    }

//
//    public void resetProducer() {
//        this.producer = null;
//    }
//
//    public KafkaConsumer<Integer, String> createConsumer(String... topicIds) {
//        return createConsumer(IntegerDeserializer.class, StringDeserializer.class, emptyMap(), topicIds);
//    }
//
//    public <K, V> KafkaConsumer<K, V> createConsumer(
//            Class<? extends Deserializer<K>> keyDeserializerClass,
//            Class<? extends Deserializer<V>> valueDeserializerClass,
//            Map<String, String> properties,
//            String... topicIds
//    ) {
//        Properties consumerProps = new Properties();
//        consumerProps.setProperty("bootstrap.servers", brokerConnectionString);
//        consumerProps.setProperty("group.id", randomString());
//        consumerProps.setProperty("client.id", "consumer0");
//        consumerProps.setProperty("key.deserializer", keyDeserializerClass.getCanonicalName());
//        consumerProps.setProperty("value.deserializer", valueDeserializerClass.getCanonicalName());
//        consumerProps.setProperty("isolation.level", "read_committed");
//        // to make sure the consumer starts from the beginning of the topic
//        consumerProps.setProperty("auto.offset.reset", "earliest");
//        consumerProps.putAll(properties);
//        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps);
//        consumer.subscribe(Arrays.asList(topicIds));
//        return consumer;
//    }
//
//    public void assertTopicContentsEventually(
//            String topic,
//            Map<Integer, String> expected,
//            boolean assertPartitionEqualsKey
//    ) {
//        try (KafkaConsumer<Integer, String> consumer = createConsumer(topic)) {
//            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
//            for (int totalRecords = 0; totalRecords < expected.size() && System.nanoTime() < timeLimit; ) {
//                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
//                for (ConsumerRecord<Integer, String> record : records) {
//                    assertEquals("key=" + record.key(), expected.get(record.key()), record.value());
//                    if (assertPartitionEqualsKey) {
//                        assertEquals(record.key().intValue(), record.partition());
//                    }
//                    totalRecords++;
//                }
//            }
//        }
//    }
//
//    public <K, V> void assertTopicContentsEventually(
//            String topic,
//            Map<K, V> expected,
//            Class<? extends Deserializer<K>> keyDeserializerClass,
//            Class<? extends Deserializer<V>> valueDeserializerClass
//    ) {
//        assertTopicContentsEventually(topic, expected, keyDeserializerClass, valueDeserializerClass, emptyMap());
//    }
//
//    public <K, V> void assertTopicContentsEventually(
//            String topic,
//            Map<K, V> expected,
//            Class<? extends Deserializer<K>> keyDeserializerClass,
//            Class<? extends Deserializer<V>> valueDeserializerClass,
//            Map<String, String> consumerProperties
//    ) {
//        try (KafkaConsumer<K, V> consumer = createConsumer(
//                keyDeserializerClass,
//                valueDeserializerClass,
//                consumerProperties,
//                topic
//        )) {
//            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
//            Set<K> seenKeys = new HashSet<>();
//            for (int totalRecords = 0; totalRecords < expected.size() && System.nanoTime() < timeLimit; ) {
//                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
//                for (ConsumerRecord<K, V> record : records) {
//                    assertTrue("key=" + record.key() + " already seen", seenKeys.add(record.key()));
//                    V expectedValue = expected.get(record.key());
//                    assertNotNull("key=" + record.key() + " received, but not expected", expectedValue);
//                    assertEquals("key=" + record.key(), expectedValue, record.value());
//                    totalRecords++;
//                }
//            }
//        }
//    }
}
