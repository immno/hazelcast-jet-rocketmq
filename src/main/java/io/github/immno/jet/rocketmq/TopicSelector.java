package io.github.immno.jet.rocketmq;

import java.io.Serializable;

/**
 * topic selector in messages
 */
@FunctionalInterface
public interface TopicSelector<T> extends Serializable {

    /**
     * extract topic
     *
     * @param message message
     * @return topic
     */
    String getTopic(T message);

}