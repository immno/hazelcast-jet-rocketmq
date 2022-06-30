package io.github.immno.jet.rocketmq;

import io.github.immno.jet.rocketmq.impl.RocketmqTestSupport;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Test
    public void testUsedTime() throws RemotingException, InterruptedException, MQClientException {
        int count = 100000;
        RocketmqTestSupport testSupport = new RocketmqTestSupport("10.75.8.150:9876");
        long start1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            testSupport.produce("QQQQ", 1, "AAA");
        }
        long end1 = System.currentTimeMillis();
        System.out.println("## Sync all  cost: " + (end1 - start1));
        DefaultMQProducer producer = testSupport.getProducer();
        ThreadPoolExecutor senderExecutor = getSenderExecutor(2);
        producer.setAsyncSenderExecutor(senderExecutor);
        CountDownLatch downLatch = new CountDownLatch(count);
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Message message = new Message("QQQQ", "AAA".getBytes(StandardCharsets.UTF_8));
            message.setKeys(String.valueOf(1));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    downLatch.countDown();
                }

                @Override
                public void onException(Throwable e) {
                    downLatch.countDown();
                    System.out.println(e.getMessage());
                }
            });
        }
        long end2 = System.currentTimeMillis();
        System.out.println("## Async send cost: " + (end2 - start2));
        downLatch.await();
        long end3 = System.currentTimeMillis();
        System.out.println("## Async all  cost: " + (end3 - start2));
    }

    private ThreadPoolExecutor getSenderExecutor(int poolSize) {
        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(150000),
                new ThreadFactory() {
                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                    }
                });
    }

    @Test
    public void name() {
        LinkedBlockingQueue<Object> objects = new LinkedBlockingQueue<>(10000);
        System.out.println(objects.remainingCapacity());

    }
}
