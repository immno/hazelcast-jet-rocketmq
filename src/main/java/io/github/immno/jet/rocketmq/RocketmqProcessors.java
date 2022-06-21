package io.github.immno.jet.rocketmq;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import io.github.immno.jet.rocketmq.impl.StreamRocketmqP;
import io.github.immno.jet.rocketmq.impl.WriteRocketmqP;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Properties;

/**
 * Refer to KafkaProcessors
 */
public final class RocketmqProcessors {
    private RocketmqProcessors() {
    }

    /**
     * Returns a supplier of processors for {@link
     * RocketmqSources#rocketmq(Properties, FunctionEx, String...)}.
     */
    public static <T> ProcessorMetaSupplier streamRocketmqP(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super MessageExt, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy,
            @Nonnull String... topics) {
        Preconditions.checkPositive(topics.length, "At least one topic must be supplied");
        return ProcessorMetaSupplier.of(
                StreamRocketmqP.PREFERRED_LOCAL_PARALLELISM,
                StreamRocketmqP.processorSupplier(properties, Arrays.asList(topics), projectionFn, eventTimePolicy));
    }

    public static <T> ProcessorMetaSupplier writeRocketmqP(
            @Nonnull Properties properties,
            @Nonnull TopicSelector<T> topic,
            @Nonnull FunctionEx<? super T, String> extractKeyFn,
            @Nonnull FunctionEx<? super T, byte[]> extractValueFn,
            boolean exactlyOnce) {
        return writeRocketmqP(properties,
                (T t) -> new Message(topic.getTopic(t), "", extractKeyFn.apply(t), extractValueFn.apply(t)),
                exactlyOnce);
    }

    public static <T> ProcessorMetaSupplier writeRocketmqP(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<? super T, ? extends Message> toRecordFn,
            boolean exactlyOnce) {
        return ProcessorMetaSupplier.of(1, WriteRocketmqP.supplier(properties, toRecordFn, exactlyOnce));
    }
}
