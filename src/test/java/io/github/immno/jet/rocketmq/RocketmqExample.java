package io.github.immno.jet.rocketmq;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

public class RocketmqExample {
    @Test
    public void consumer() throws MQClientException {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("pushConsumer");
        pushConsumer.setNamesrvAddr("10.75.8.151:9876");
        pushConsumer.setMessageModel(MessageModel.CLUSTERING);

        pushConsumer.subscribe("mno_test", "Tag1 || Tag2");
        pushConsumer.setInstanceName("Jet-Mno");

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        pushConsumer.start();
        System.out.printf("Broadcast Consumer Started.%n");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void producer() throws MQClientException, MQBrokerException, RemotingException, UnsupportedEncodingException,
            InterruptedException {
        System.out.println("SyncProducer start......");
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("mno_test");
        defaultMQProducer.setNamesrvAddr("10.75.8.151:9876");
        defaultMQProducer.start();
        int count = 0;
        while (count < 10000) {
            send(defaultMQProducer, count, count % 3);
            Thread.sleep(100);
            count++;
        }
        defaultMQProducer.shutdown();
        System.out.println("SyncProducer end......");

    }

    private static void send(DefaultMQProducer defaultMQProducer, Integer i, int tag) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException, UnsupportedEncodingException {
        SendResult sendResult = defaultMQProducer.send(new Message("mno_test", "Tag" + tag,
                ("hello this is sync message_" + i + "!").getBytes(RemotingHelper.DEFAULT_CHARSET)));
        System.out.println(sendResult);
    }
}
