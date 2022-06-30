package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.logging.ILogger;
import io.github.immno.jet.rocketmq.RocketmqConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import javax.annotation.Nonnull;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * RocketMQ ensures that all messages are delivered at least once.
 * In most cases, the messages are not repeated.
 */
public class WriteRocketmqP<T> extends AbstractProcessor {
    public static final int TXN_POOL_SIZE = 2;
    public static final int QUEUE_CAPACITY = 10000;
    private final Properties properties;
    private final Function<? super T, ? extends Message> toRecordFn;
    private SendCallback callback;

    private DefaultMQProducer producer;
    private ThreadPoolExecutor senderExecutor;

    private WriteRocketmqP(
            @Nonnull Properties properties,
            @Nonnull Function<? super T, ? extends Message> toRecordFn,
            boolean exactlyOnce) {
        this.properties = properties;
        this.toRecordFn = toRecordFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        this.callback = new SendCallbackImpl(context.logger());
        String defaultGroup = String.join("-", "Jet", String.valueOf(context.jobId()), String.valueOf(context.globalProcessorIndex()));
        this.producer = new DefaultMQProducer(RocketmqConfig.buildAclRPCHook(this.properties));
        this.producer.setInstanceName(defaultGroup);
        this.senderExecutor = getSenderExecutor();
        this.producer.setAsyncSenderExecutor(this.senderExecutor);
        RocketmqConfig.buildProducerConfigs(properties, this.producer);
        try {
            this.producer.start();
        } catch (MQClientException e) {
            context.logger().warning("Unable to start consumer", e);
            throw e;
        }
    }

    private ThreadPoolExecutor getSenderExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                TXN_POOL_SIZE,
                TXN_POOL_SIZE,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                new ThreadFactory() {
                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                    }
                });
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        @SuppressWarnings("unchecked")
        Message message = toRecordFn.apply((T) item);
        return produce(message);
    }

    @Override
    public boolean complete() {
        return senderExecutor.getQueue().isEmpty();
    }

    private boolean produce(Message message) {
        try {
            this.producer.send(message, callback);
        } catch (MQClientException | RemotingException | InterruptedException e) {
            getLogger().warning("Unable to send data", e);
            return false;
        }
        return true;
    }

    private static class SendCallbackImpl implements SendCallback {
        private final ILogger logger;

        private SendCallbackImpl(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
        }

        @Override
        public void onException(Throwable e) {
            if (e != null && logger != null) {
                logger.warning("Async send message failure!", e);
            }
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
        return () -> new WriteRocketmqP<>(properties, toRecordFn, false);
    }
}