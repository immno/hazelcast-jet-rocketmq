package io.github.immno.jet.rocketmq;

import com.hazelcast.internal.util.Preconditions;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * RocketMQConfig for Consumer/Producer.
 */
public class RocketmqConfig {
    // Server Config
    public static final String NAME_SERVER_ADDR = "nameserver.address"; // Required

    public static final String NAME_SERVER_POLL_INTERVAL = "nameserver.poll.interval";
    public static final int DEFAULT_NAME_SERVER_POLL_INTERVAL = 30000; // 30 seconds

    public static final String BROKER_HEART_BEAT_INTERVAL = "brokerserver.heartbeat.interval";
    public static final int DEFAULT_BROKER_HEART_BEAT_INTERVAL = 30000; // 30 seconds

    // Access control config
    public static final String ACCESS_KEY = "access.key";
    public static final String SECRET_KEY = "secret.key";

    public static final String ACCESS_CHANNEL = "access.channel";
    public static final AccessChannel DEFAULT_ACCESS_CHANNEL = AccessChannel.LOCAL;

    // Producer related config
    public static final String PRODUCER_TOPIC = "producer.topic";
    public static final String PRODUCER_GROUP = "producer.group";

    public static final String PRODUCER_RETRY_TIMES = "producer.retry.times";
    public static final int DEFAULT_PRODUCER_RETRY_TIMES = 3;

    public static final String PRODUCER_TIMEOUT = "producer.timeout";
    public static final int DEFAULT_PRODUCER_TIMEOUT = 3000; // 3 seconds

    // Consumer related config
    public static final String CONSUMER_GROUP = "consumer.group"; // Required
    public static final String CONSUMER_TOPIC = "consumer.topic"; // Required

    public static final String CONSUMER_TAG = "consumer.tag";
    public static final String CONSUMER_SQL = "consumer.sql";
    public static final String DEFAULT_CONSUMER_TAG = "*";

    public static final String CONSUMER_OFFSET_RESET_TO = "consumer.offset.reset.to";
    public static final String CONSUMER_OFFSET_LATEST = "latest";
    public static final String CONSUMER_OFFSET_EARLIEST = "earliest";
    public static final String CONSUMER_OFFSET_TIMESTAMP = "timestamp";
    public static final String CONSUMER_OFFSET_FROM_TIMESTAMP = "consumer.offset.from.timestamp";

    public static final String CONSUMER_OFFSET_PERSIST_INTERVAL = "consumer.offset.persist.interval";
    public static final int DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL = 5000; // 5 seconds

    public static final String CONSUMER_BATCH_SIZE = "consumer.batch.size";
    public static final int DEFAULT_CONSUMER_BATCH_SIZE = 32;

    public static final String CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND = "consumer.delay.when.message.not.found";
    public static final int DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND = 100;

    public static final String CONSUMER_INDEX_OF_THIS_SUB_TASK = "consumer.index";

    public static final String UNIT_NAME = "unit.name";

    public static final String WATERMARK = "watermark";

    // Delay message related config
    public static final String MSG_DELAY_LEVEL = "msg.delay.level";
    public static final int MSG_DELAY_LEVEL00 = 0; // no delay
    public static final int MSG_DELAY_LEVEL01 = 1; // 1s
    public static final int MSG_DELAY_LEVEL02 = 2; // 5s
    public static final int MSG_DELAY_LEVEL03 = 3; // 10s
    public static final int MSG_DELAY_LEVEL04 = 4; // 30s
    public static final int MSG_DELAY_LEVEL05 = 5; // 1min
    public static final int MSG_DELAY_LEVEL06 = 6; // 2min
    public static final int MSG_DELAY_LEVEL07 = 7; // 3min
    public static final int MSG_DELAY_LEVEL08 = 8; // 4min
    public static final int MSG_DELAY_LEVEL09 = 9; // 5min
    public static final int MSG_DELAY_LEVEL10 = 10; // 6min
    public static final int MSG_DELAY_LEVEL11 = 11; // 7min
    public static final int MSG_DELAY_LEVEL12 = 12; // 8min
    public static final int MSG_DELAY_LEVEL13 = 13; // 9min
    public static final int MSG_DELAY_LEVEL14 = 14; // 10min
    public static final int MSG_DELAY_LEVEL15 = 15; // 20min
    public static final int MSG_DELAY_LEVEL16 = 16; // 30min
    public static final int MSG_DELAY_LEVEL17 = 17; // 1h
    public static final int MSG_DELAY_LEVEL18 = 18; // 2h

    /**
     * Build Producer Configs.
     *
     * @param props    Properties
     * @param producer DefaultMQProducer
     */
    public static void buildProducerConfigs(Properties props, DefaultMQProducer producer) {
        buildCommonConfigs(props, producer);

        producer.setRetryTimesWhenSendFailed(
                getInteger(props, PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setRetryTimesWhenSendAsyncFailed(
                getInteger(props, PRODUCER_RETRY_TIMES, DEFAULT_PRODUCER_RETRY_TIMES));
        producer.setSendMsgTimeout(getInteger(props, PRODUCER_TIMEOUT, DEFAULT_PRODUCER_TIMEOUT));
    }

    /**
     * Build Consumer Configs.
     *
     * @param props    Properties
     * @param consumer DefaultMQPullConsumer
     */
    public static void buildConsumerConfigs(Properties props, DefaultLitePullConsumer consumer) {
        buildCommonConfigs(props, consumer);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setPersistConsumerOffsetInterval(
                getInteger(
                        props,
                        CONSUMER_OFFSET_PERSIST_INTERVAL,
                        DEFAULT_CONSUMER_OFFSET_PERSIST_INTERVAL));
        setConsumeFromWhere(props, consumer);
    }

    private static void setConsumeFromWhere(Properties props, DefaultLitePullConsumer consumer) {
        String initialOffset = props.getProperty(CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        if (Objects.equals(initialOffset, CONSUMER_OFFSET_EARLIEST)) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        } else if (Objects.equals(initialOffset, CONSUMER_OFFSET_LATEST)) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        } else if (Objects.equals(initialOffset, CONSUMER_OFFSET_TIMESTAMP)) {
            String timestamp = props.getProperty(CONSUMER_OFFSET_FROM_TIMESTAMP);
            Preconditions.checkHasText(timestamp, CONSUMER_OFFSET_FROM_TIMESTAMP + " is empty");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
            consumer.setConsumeTimestamp(timestamp);
        } else {
            throw new RuntimeException("Invalid ConsumeFromWhere Value", null);
        }
    }

    /**
     * Build Common Configs.
     *
     * @param props  Properties
     * @param client ClientConfig
     */
    public static void buildCommonConfigs(Properties props, ClientConfig client) {
        String nameServers = props.getProperty(NAME_SERVER_ADDR);
        Preconditions.checkHasText(nameServers, "NameServers is empty");
        client.setNamesrvAddr(nameServers);
        client.setHeartbeatBrokerInterval(
                getInteger(props, BROKER_HEART_BEAT_INTERVAL, DEFAULT_BROKER_HEART_BEAT_INTERVAL));
        // When using aliyun products, you need to set up channels
        client.setAccessChannel(getAccessChannel(props, ACCESS_CHANNEL, DEFAULT_ACCESS_CHANNEL));
        client.setUnitName(props.getProperty(UNIT_NAME, null));
    }

    /**
     * Build credentials for client.
     *
     * @param props
     * @return
     */
    public static AclClientRPCHook buildAclRPCHook(Properties props) {
        String accessKey = props.getProperty(ACCESS_KEY);
        String secretKey = props.getProperty(SECRET_KEY);
        if (!isEmpty(accessKey) && !isEmpty(secretKey)) {
            return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        return null;
    }

    public static int getInteger(Properties props, String key, int defaultValue) {
        return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static long getLong(Properties props, String key, long defaultValue) {
        return Long.parseLong(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static AccessChannel getAccessChannel(
            Properties props, String key, AccessChannel defaultValue) {
        return AccessChannel.valueOf(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static String getInstanceName(String... args) {
        if (null != args && args.length > 0) {
            return String.join("_", args);
        }
        return ManagementFactory.getRuntimeMXBean().getName() + "_" + System.nanoTime();
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
}
