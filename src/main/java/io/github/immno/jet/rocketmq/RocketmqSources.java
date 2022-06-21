package io.github.immno.jet.rocketmq;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;

import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.StreamSource;

/**
 * Refer to KafkaSources
 */
public final class RocketmqSources {
    private RocketmqSources() {
    }

    /**
     * Convenience for {@link #rocketmq(Properties, FunctionEx, String...)}
     * wrapping the output byte[].
     */
    @Nonnull
    public static StreamSource<byte[]> rocketmq(
            @Nonnull Properties properties,
            @Nonnull String... topics) {
        return RocketmqSources.rocketmq(properties, Message::getBody, topics);
    }

    @Nonnull
    public static <T> StreamSource<T> rocketmq(
            @Nonnull Properties properties,
            @Nonnull FunctionEx<MessageExt, T> projectionFn,
            @Nonnull String... topics) {
        checkPositive(topics.length, "At least one topic required");
        return streamFromProcessorWithWatermarks("rocketmqSource(" + String.join(",", topics) + ")",
                true, w -> RocketmqProcessors.streamRocketmqP(properties, projectionFn, w, topics));
    }
}