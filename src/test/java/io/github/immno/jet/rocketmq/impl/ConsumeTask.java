package io.github.immno.jet.rocketmq.impl;


@FunctionalInterface
public interface ConsumeTask<T> {
    void run(T t) throws Exception;
}