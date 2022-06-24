package io.github.immno.jet.rocketmq;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.StreamSource;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;

/**
 * Refer to KafkaSources
 */
public final class RocketmqSources {
    private RocketmqSources() {
    }

    @Nonnull
    public static StreamSource<Map.Entry<String, String>> rocketmq(
            @Nonnull Properties properties,
            @Nonnull String... topics) {
        return RocketmqSources.rocketmq(properties, r -> Util.entry(r.getKeys(), new String(r.getBody())), topics);
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