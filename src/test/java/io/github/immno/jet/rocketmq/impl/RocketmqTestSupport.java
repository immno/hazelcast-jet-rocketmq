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
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.test.util.MQAdmin;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;

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

    public DefaultMQProducer getProducer() {
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

    public void resetProducer() {
        this.producer = null;
    }

    public void consumer(List<String> topicIds, ConsumeTask<DefaultLitePullConsumer> task) {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(HazelcastTestSupport.randomString());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr(namesrvAddr);
        try {
            consumer.start();
            for (String topicId : topicIds) {
                consumer.subscribe(topicId, "*");
            }
            task.run(consumer);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.shutdown();
        }
    }

    public void assertTopicContentsEventually(
            String topic,
            Map<Integer, String> expected) {
        consumer(Collections.singletonList(topic), consumer -> {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            for (int totalRecords = 0; totalRecords < expected.size() && System.nanoTime() < timeLimit; ) {
                List<MessageExt> records = consumer.poll(Duration.ofMillis(100).toMillis());
                for (MessageExt record : records) {
                    Assert.assertEquals("key=" + record.getKeys(), expected.get(Integer.valueOf(record.getKeys())), new String(record.getBody()));
                    totalRecords++;
                }
            }
        });
    }

}
