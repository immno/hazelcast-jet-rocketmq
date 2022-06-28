package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import io.github.immno.jet.rocketmq.RocketmqConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;


public class WriteRocketmqP<T> extends AbstractProcessor {
    public static final int TXN_POOL_SIZE = 2;
    private final Properties properties;
    private final Function<? super T, ? extends Message> toRecordFn;
    private final boolean exactlyOnce;

    private DefaultMQProducer producer;

    private WriteRocketmqP(
            @Nonnull Properties properties,
            @Nonnull Function<? super T, ? extends Message> toRecordFn,
            boolean exactlyOnce) {
        this.properties = properties;
        this.toRecordFn = toRecordFn;
        this.exactlyOnce = exactlyOnce;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        String defaultGroup = String.join("-", "Jet", String.valueOf(context.jobId()),
                context.vertexName(), String.valueOf(context.localProcessorIndex()));
        String group = properties.getProperty(RocketmqConfig.CONSUMER_GROUP, defaultGroup);

        this.producer = new DefaultMQProducer(group, RocketmqConfig.buildAclRPCHook(this.properties));
        this.producer.setInstanceName("Jet-" + context.jobId() + "-" + context.localProcessorIndex());
        RocketmqConfig.buildProducerConfigs(properties, this.producer);
        try {
            this.producer.start();
        } catch (MQClientException e) {
            getLogger().warning("Unable to start consumer", e);
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        List<Message> messages = new ArrayList<>();
        for (Object item; (item = inbox.peek()) != null; ) {
            @SuppressWarnings("unchecked")
            Message message = toRecordFn.apply((T) item);
            if (message != null) {
                messages.add(message);
            }
        }
        try {
            this.producer.send(messages);
            inbox.clear();
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            getLogger().warning("Unable to send data", e);
        }
    }

    @Override
    public void close() {
        if (this.producer != null) {
            this.producer.shutdown();
        }
    }

    public static <T> SupplierEx<Processor> supplier(
            @Nonnull Properties properties,
            @Nonnull Function<? super T, ? extends Message> toRecordFn,
            boolean exactlyOnce) {
        return () -> new WriteRocketmqP<>(properties, toRecordFn, exactlyOnce);
    }

    private static class MessageWarp implements Serializable {
        private final Message message;

        private MessageWarp(Message message) {
            this.message = message;
        }

        private MessageWarp of(Message message) {
            return new MessageWarp(message);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MessageWarp)) return false;
            MessageWarp that = (MessageWarp) o;
            return Objects.equals(message.getTopic(), that.message.getTopic()) &&
                    Objects.equals(message.getKeys(), that.message.getKeys()) &&
                    Arrays.equals(message.getBody(), that.message.getBody()) &&
                    Objects.equals(message.getTags(), that.message.getTags()) &&
                    Objects.equals(message.getFlag(), that.message.getFlag()) &&
                    Objects.equals(message.getTransactionId(), that.message.getTransactionId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(message.getTopic(),
                    message.getKeys(),
                    Arrays.hashCode(message.getBody()),
                    message.getTags(),
                    message.getFlag(),
                    message.getTransactionId());
        }
    }
}