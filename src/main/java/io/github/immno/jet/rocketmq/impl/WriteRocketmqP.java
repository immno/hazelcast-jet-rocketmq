package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import io.github.immno.jet.rocketmq.RocketmqConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import javax.annotation.Nonnull;
import java.util.Properties;
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
        this.producer = new DefaultMQProducer(RocketmqConfig.buildAclRPCHook(this.properties));
        this.producer.setInstanceName("Jet-" + context.globalProcessorIndex());
        RocketmqConfig.buildProducerConfigs(properties, this.producer);
        try {
            this.producer.start();
        } catch (MQClientException e) {
            getLogger().warning("Unable to start consumer", e);
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        @SuppressWarnings("unchecked")
        Message message = toRecordFn.apply((T) item);
        this.producer.send(message);
        return true;
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

}