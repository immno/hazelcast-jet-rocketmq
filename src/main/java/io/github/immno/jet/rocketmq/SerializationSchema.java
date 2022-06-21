package io.github.immno.jet.rocketmq;

import java.io.Serializable;

/**
 * Message (keys and body) serialization
 */
public interface SerializationSchema<T> extends Serializable {

    /**
     * serialization of keys
     *
     * @param message message
     * @return keys
     */
    default String serializeKeys(T message) {
        return "";
    }

    /**
     * serialization of body
     *
     * @param message message
     * @return body
     */
    byte[] serializeValue(T message);
}