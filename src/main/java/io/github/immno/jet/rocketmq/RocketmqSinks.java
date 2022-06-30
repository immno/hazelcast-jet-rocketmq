package io.github.immno.jet.rocketmq;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.rocketmq.common.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

/**
 * Refer to KafkaSinks
 */
public final class RocketmqSinks {

   private RocketmqSinks() {
   }

   @Nonnull
   public static <E> Sink<E> rocketmq(
           @Nonnull Properties properties,
           @Nonnull FunctionEx<? super E, Message> toRecordFn) {
       return Sinks.fromProcessor("rocketmqSink", RocketmqProcessors.writeRocketmqP(properties, toRecordFn, true));
   }

   @Nonnull
   public static <E> Sink<E> rocketmq(
           @Nonnull Properties properties,
           @Nonnull String topic,
           @Nonnull SerializationSchema<E> schema) {
       return rocketmq(properties, message -> topic, schema);
   }

   @Nonnull
   public static <E> Sink<E> rocketmq(
           @Nonnull Properties properties,
           @Nonnull TopicSelector<E> topic,
           @Nonnull SerializationSchema<E> schema) {
       return Sinks.fromProcessor("rocketmqSinkTopicSelector",
               RocketmqProcessors.writeRocketmqP(properties, topic, schema::serializeKeys, schema::serializeValue, true));
   }

   @Nonnull
   public static <E> Sink<E> rocketmq(
           @Nonnull Properties properties,
           @Nonnull String topic,
           @Nonnull FunctionEx<? super E, String> extractKeyFn,
           @Nonnull FunctionEx<? super E, byte[]> extractValueFn) {
       return Sinks.fromProcessor("rocketmqSink(" + topic + ")",
               RocketmqProcessors.writeRocketmqP(properties, message -> topic, extractKeyFn, extractValueFn, true));
   }

   @Nonnull
   public static Sink<String> rocketmq(@Nonnull Properties properties, @Nonnull String topic) {
       return rocketmq(properties, topic, s -> "", String::getBytes);
   }

   @Nonnull
   public static <E> Builder<E> rocketmq(@Nonnull Properties properties) {
       return new Builder<>(properties);
   }

   public static final class Builder<E> {

       private final Properties properties;
       private FunctionEx<? super E, ? extends Message> toRecordFn;
       private TopicSelector<E> topic;
       private FunctionEx<? super E, String> extractKeyFn;
       private FunctionEx<? super E, byte[]> extractValueFn;
       private boolean exactlyOnce = true;

       private Builder(Properties properties) {
           this.properties = properties;
       }

       @Nonnull
       public Builder<E> topic(String topic) {
           if (toRecordFn != null) {
               throw new IllegalArgumentException("toRecordFn already set, you can't use topic if it's set");
           }
           this.topic = message -> topic;
           return this;
       }

       @Nonnull
       public Builder<E> topic(TopicSelector<E> topic) {
           if (toRecordFn != null) {
               throw new IllegalArgumentException("toRecordFn already set, you can't use topic if it's set");
           }
           this.topic = topic;
           return this;
       }

       @Nonnull
       public Builder<E> extractKeyFn(@Nonnull FunctionEx<? super E, String> extractKeyFn) {
           if (toRecordFn != null) {
               throw new IllegalArgumentException("toRecordFn already set, you can't use extractKeyFn if it's set");
           }
           this.extractKeyFn = extractKeyFn;
           return this;
       }

       @Nonnull
       public Builder<E> extractValueFn(@Nonnull FunctionEx<? super E, byte[]> extractValueFn) {
           if (toRecordFn != null) {
               throw new IllegalArgumentException("toRecordFn already set, you can't use extractValueFn if it's set");
           }
           this.extractValueFn = extractValueFn;
           return this;
       }

       @Nonnull
       public Builder<E> toRecordFn(@Nullable FunctionEx<? super E, ? extends Message> toRecordFn) {
           if (topic != null || extractKeyFn != null || extractValueFn != null) {
               throw new IllegalArgumentException("topic, extractKeyFn or extractValueFn are already set, you can't use" +
                       " toRecordFn along with them");
           }
           this.toRecordFn = toRecordFn;
           return this;
       }

       @Nonnull
       public Builder<E> exactlyOnce(boolean enable) {
           exactlyOnce = enable;
           return this;
       }

       /**
        * Builds the Sink object that you pass to the {@link
        * GeneralStage#writeTo(Sink)} method.
        */
       @Nonnull
       public Sink<E> build() {
           if ((extractValueFn != null || extractKeyFn != null) && topic == null) {
               throw new IllegalArgumentException("if `extractKeyFn` or `extractValueFn` are set, `topic` must be set " +
                       "too");
           }
           if (topic == null && toRecordFn == null) {
               throw new IllegalArgumentException("either `topic` or `toRecordFn` must be set");
           }
           if (topic != null) {
               FunctionEx<? super E, String> extractKeyFn1 = extractKeyFn != null ? extractKeyFn : t -> null;
               FunctionEx<? super E, byte[]> extractValueFn1 = extractValueFn != null ? extractValueFn : t -> new byte[]{};
               return Sinks.fromProcessor("rocketmqSink(" + topic + ")",
                       RocketmqProcessors.writeRocketmqP(properties, topic, extractKeyFn1, extractValueFn1, exactlyOnce));
           } else {
               ProcessorMetaSupplier metaSupplier = RocketmqProcessors.writeRocketmqP(properties, toRecordFn, exactlyOnce);
               return Sinks.fromProcessor("rocketmqSink", metaSupplier);
           }
       }
   }
}