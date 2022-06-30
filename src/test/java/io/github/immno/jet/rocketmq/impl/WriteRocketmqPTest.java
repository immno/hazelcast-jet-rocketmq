package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.SinkStressTestUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import io.github.immno.jet.rocketmq.RocketmqConfig;
import io.github.immno.jet.rocketmq.RocketmqSinks;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteRocketmqPTest extends SimpleTestInClusterSupport {

    private static final int QUEUE_COUNT = 4;

    private static RocketmqTestSupport rocketmqTestSupport;

    private static String namesrvAddr;
    private final String sourceIMapName = randomMapName();
    private Properties properties;
    private String topic;
    private IMap<Integer, String> sourceIMap;


    @BeforeClass
    public static void beforeClass() throws IOException {
        namesrvAddr = "10.75.8.150:9876";
        rocketmqTestSupport = new RocketmqTestSupport(namesrvAddr);
        initialize(2, null);
    }

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty(RocketmqConfig.NAME_SERVER_ADDR, namesrvAddr);

        topic = randomName();
        rocketmqTestSupport.createTopic(topic, QUEUE_COUNT);

        sourceIMap = instance().getMap(sourceIMapName);
        for (int i = 0; i < 4; i++) {
            sourceIMap.put(i, String.valueOf(i));
        }
        System.out.println("Topic: " + topic);
    }

    @AfterClass
    public static void afterClass() {
        if (rocketmqTestSupport != null) {
            rocketmqTestSupport.shutdownKafkaCluster();
            rocketmqTestSupport = null;
        }
    }

    @Test
    public void testWriteToTopic() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(sourceIMap))
                .writeTo(RocketmqSinks.rocketmq(properties, topic,
                        message -> String.valueOf(message.getKey()),
                        message -> message.getValue().getBytes(StandardCharsets.UTF_8)));
        instance().getJet().newJob(p).join();

        rocketmqTestSupport.assertTopicContentsEventually(topic, sourceIMap);
    }

    @Test
    public void when_recordLingerEnabled_then_sentOnCompletion() {

        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Map.Entry<Integer, String>>batchFromProcessor("source",
                        ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
                .writeTo(RocketmqSinks.rocketmq(properties, new DelayMessageFunction(topic)));

        Job job = instance().getJet().newJob(p);

        // the event should not appear in the topic due to linger.ms
        rocketmqTestSupport.consumer(Collections.singletonList(topic), consumer -> {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(100).size()), 2);
        });


        // Then
        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
        logger.info("Job finished");
        rocketmqTestSupport.assertTopicContentsEventually(topic, singletonMap(0, "v"));
    }

    @Test
    public void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot_exactlyOnce() {
        when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(true);
    }

    @Test
    public void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot_atLeastOnce() {
        when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(false);
    }

    private void when_processingGuaranteeOn_then_lingeringRecordsSentOnSnapshot(boolean exactlyOnce) {
        // Given
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Map.Entry<Integer, String>>batchFromProcessor("source",
                        ProcessorMetaSupplier.of(ProcessorWithEntryAndLatch::new)))
                .writeTo(RocketmqSinks.<Map.Entry<Integer, String>>rocketmq(properties)
                        .toRecordFn(new DelayMessageFunction(topic))
                        .exactlyOnce(exactlyOnce)
                        .build());

        Job job = instance().getJet().newJob(p, new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(4000));

        // the event should not appear in the topic due to linger.ms
        rocketmqTestSupport.consumer(Collections.singletonList(topic), consumer -> {
            assertTrueAllTheTime(() -> assertEquals(0, consumer.poll(100).size()), 10);
        });

        // Then
        ProcessorWithEntryAndLatch.allowSnapshot = true;
        rocketmqTestSupport.assertTopicContentsEventually(topic, singletonMap(0, "v"));

        ProcessorWithEntryAndLatch.isDone = true;
        job.join();
    }

    private static class DelayMessageFunction implements FunctionEx<Map.Entry<Integer, String>, Message> {
        private final String topic;

        public DelayMessageFunction(String topic) {
            this.topic = topic;
        }

        @Override
        public Message applyEx(Map.Entry<Integer, String> entry) throws Exception {
            Message message = new Message(topic, String.valueOf(entry.getKey()),
                    entry.getValue().getBytes(StandardCharsets.UTF_8));
            message.setDelayTimeLevel(RocketmqConfig.MSG_DELAY_LEVEL04);
            return message;
        }
    }

    @Test
    public void stressTest_graceful_exOnce() {
        stressTest(true, true);
    }

    @Test
    public void stressTest_forceful_exOnce() {
        // RocketMQ ensures that all messages are delivered at least once. In most cases, the messages are not repeated.
        // https://rocketmq.apache.org/docs/faq/
//        stressTest(false, true);
    }

    @Test
    public void stressTest_graceful_atLeastOnce() {
        stressTest(true, false);
    }

    @Test
    public void stressTest_forceful_atLeastOnce() {
        stressTest(false, false);
    }

    private void stressTest(boolean graceful, boolean exactlyOnce) {
        String topicLocal = topic;
        Sink<Integer> sink = RocketmqSinks.<Integer>rocketmq(properties)
                .toRecordFn(v -> new Message(topicLocal, "", "0", String.valueOf(v).getBytes(StandardCharsets.UTF_8)))
                .exactlyOnce(exactlyOnce)
                .build();

        rocketmqTestSupport.consumer(Collections.singletonList(topic), consumer -> {
            List<Integer> actualSinkContents = new ArrayList<>();
            SinkStressTestUtil.test_withRestarts(instance(), logger, sink, graceful, exactlyOnce, () -> {
                for (List<MessageExt> records; !(records = consumer.poll(10)).isEmpty(); ) {
                    for (MessageExt record : records) {
                        actualSinkContents.add(Integer.parseInt(new String(record.getBody())));
                    }
                }
                return actualSinkContents;
            });
        });
    }

    private static final class ProcessorWithEntryAndLatch extends AbstractProcessor {
        static volatile boolean isDone;
        static volatile boolean allowSnapshot;

        private Traverser<Map.Entry<Integer, String>> t = Traversers.singleton(entry(0, "v"));

        private ProcessorWithEntryAndLatch() {
            // reset so that values from previous run don't remain
            isDone = false;
            allowSnapshot = false;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            return allowSnapshot;
        }

        @Override
        public boolean complete() {
            // emit the item and wait for the latch to complete
            return emitFromTraverser(t) && isDone;
        }
    }
}
