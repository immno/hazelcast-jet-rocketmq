package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.*;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import io.github.immno.jet.rocketmq.RocketmqConfig;
import io.github.immno.jet.rocketmq.RocketmqSources;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.*;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.*;

@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamRocketmqPTest extends SimpleTestInClusterSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;
    private static final long LAG = 3;

    private static RocketmqTestSupport rocketmqTestSupport;

    private static String namesrvAddr;
    private String topic1Name;
    private String topic2Name;

    @BeforeClass
    public static void beforeClass() {
        namesrvAddr = "10.75.8.150:9876";
        rocketmqTestSupport = new RocketmqTestSupport(namesrvAddr);
        initialize(2, null);
    }

    @Before
    public void before() {
        topic1Name = randomString();
        topic2Name = randomString();
        System.out.println("Topics: " + topic1Name + " , " + topic2Name);
        rocketmqTestSupport.createTopic(topic1Name, INITIAL_PARTITION_COUNT);
        rocketmqTestSupport.createTopic(topic2Name, INITIAL_PARTITION_COUNT);
    }

    @AfterClass
    public static void afterClass() {
        rocketmqTestSupport.shutdownKafkaCluster();
        rocketmqTestSupport = null;
    }

    @Test
    public void when_projectionFunctionProvided_thenAppliedToReadRecords() {
        int messageCount = 20;
        Pipeline p = Pipeline.create();
        p.readFrom(RocketmqSources.rocketmq(properties(), rec -> new String(rec.getBody()) + "-x", topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list("sink"));

        instance().getJet().newJob(p);
        sleepAtLeastSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            rocketmqTestSupport.produce(topic1Name, i, Integer.toString(i));
        }
        IList<String> list = instance().getList("sink");
        assertTrueEventually(() -> {
            assertEquals(messageCount, list.size());
            for (int i = 0; i < messageCount; i++) {
                String value = i + "-x";
                assertTrue("missing entry: " + value, list.contains(value));
            }
        }, 5);
    }

    @Test
    public void integrationTest_noSnapshotting() throws Exception {
        integrationTest(ProcessingGuarantee.NONE);
    }

    @Test
    public void integrationTest_withSnapshotting() throws Exception {
        integrationTest(EXACTLY_ONCE);
    }

    private void integrationTest(ProcessingGuarantee guarantee) throws Exception {
        int messageCount = 20;
        HazelcastInstance[] instances = new HazelcastInstance[2];
        Arrays.setAll(instances, i -> createHazelcastInstance());

        Pipeline p = Pipeline.create();
        p.readFrom(RocketmqSources.rocketmq(properties(),
                        r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), topic1Name, topic2Name))
                .withoutTimestamps()
                .writeTo(Sinks.list("sink"));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(guarantee);
        config.setSnapshotIntervalMillis(500);
        Job job = instances[0].getJet().newJob(p, config);
        sleepSeconds(3);
        for (int i = 0; i < messageCount; i++) {
            rocketmqTestSupport.produce(topic1Name, i, Integer.toString(i));
            rocketmqTestSupport.produce(topic2Name, i - messageCount, Integer.toString(i - messageCount));
        }
        IList<Object> list = instances[0].getList("sink");

        assertTrueEventually(() -> {
            assertEquals(messageCount * 2, list.size());
            for (int i = 0; i < messageCount; i++) {
                Map.Entry<Integer, String> entry1 = createEntry(i);
                Map.Entry<Integer, String> entry2 = createEntry(i - messageCount);
                assertTrue("missing entry: " + entry1, list.contains(entry1));
                assertTrue("missing entry: " + entry2, list.contains(entry2));
            }
        }, 15);

        if (guarantee != ProcessingGuarantee.NONE) {
            // wait until a new snapshot appears
            JobRepository jr = new JobRepository(instances[0]);
            long currentMax = jr.getJobExecutionRecord(job.getId()).snapshotId();
            assertTrueEventually(() -> {
                JobExecutionRecord jobExecutionRecord = jr.getJobExecutionRecord(job.getId());
                assertNotNull("jobExecutionRecord == null", jobExecutionRecord);
                long newMax = jobExecutionRecord.snapshotId();
                assertTrue("no snapshot produced", newMax > currentMax);
                System.out.println("snapshot " + newMax + " found, previous was " + currentMax);
            });

            // Bring down one member. Job should restart and drain additional items (and maybe
            // some of the previous duplicately).
            instances[1].getLifecycleService().terminate();
            Thread.sleep(500);

            for (int i = messageCount; i < 2 * messageCount; i++) {
                rocketmqTestSupport.produce(topic1Name, i, Integer.toString(i));
                rocketmqTestSupport.produce(topic2Name, i - messageCount, Integer.toString(i - messageCount));
            }

            assertTrueEventually(() -> {
                assertEquals("Not all messages were received", messageCount * 4, list.size());
                for (int i = 0; i < 2 * messageCount; i++) {
                    Map.Entry<Integer, String> entry1 = createEntry(i);
                    Map.Entry<Integer, String> entry2 = createEntry(i - messageCount);
                    assertTrue("missing entry: " + entry1, list.contains(entry1));
                    assertTrue("missing entry: " + entry2, list.contains(entry2));
                }
            }, 20);
        }

        assertFalse(job.getFuture().isDone());

        // cancel the job
        job.cancel();
        assertTrueEventually(() -> assertTrue(job.getFuture().isDone()));
    }

//    @Test
//    public void when_eventsInAllPartitions_then_watermarkOutputImmediately() throws Exception {
//        StreamRocketmqP processor = createProcessor(properties(), 1, r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 10_000);
//        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
//        processor.init(outbox, new TestProcessorContext());
//
//        for (int i = 0; i < INITIAL_PARTITION_COUNT; i++) {
//            Map.Entry<Integer, String> event = entry(i + 100, Integer.toString(i));
//            System.out.println("produced event " + event);
//            rocketmqTestSupport.produce(topic1Name, i, null, event.getKey(), event.getValue());
//            if (i == INITIAL_PARTITION_COUNT - 1) {
//                assertEquals(new Watermark(100 - LAG), consumeEventually(processor, outbox));
//            }
//            assertEquals(event, consumeEventually(processor, outbox));
//        }
//    }

//    @Test
//    public void when_noAssignedPartitionAndAddedLater_then_resumesFromIdle() throws Exception {
//        // we ask to create 5th out of 5 processors, but we have only 4 partitions and 1 topic
//        // --> our processor will have nothing assigned
//        StreamRocketmqP processor = createProcessor(properties(), 1, entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 10_000);
//        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
//        processor.init(outbox, new TestProcessorContext()
//                .setTotalParallelism(INITIAL_PARTITION_COUNT + 1)
//                .setGlobalProcessorIndex(INITIAL_PARTITION_COUNT));
//
//        assertTrue(processor.currentAssignment.isEmpty());
//        assertEquals(IDLE_MESSAGE, consumeEventually(processor, outbox));
//
//        // add a partition and produce an event to it
//        rocketmqTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 1);
//        Map.Entry<Integer, String> value = produceEventToNewPartition(INITIAL_PARTITION_COUNT);
//
//        Object actualEvent;
//        do {
//            actualEvent = consumeEventually(processor, outbox);
//        } while (actualEvent instanceof Watermark);
//        assertEquals(value, actualEvent);
//    }

    @Test
    public void when_eventsInSinglePartition_then_watermarkAfterIdleTime() throws Exception {
        // When
        StreamRocketmqP<Map.Entry<Integer, String>> processor = createProcessor(properties(), 2,
                r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        rocketmqTestSupport.produce(topic1Name, 10, "foo");

        // Then
        assertEquals(entry(10, "foo"), consumeEventually(processor, outbox));
        long time1 = System.nanoTime();
        assertEquals(new Watermark(10 - LAG), consumeEventually(processor, outbox));
        long time2 = System.nanoTime();
        long elapsedMs = NANOSECONDS.toMillis(time2 - time1);
        assertBetween("elapsed time", elapsedMs, 3000, 30_000);
    }

    @Test
    public void when_snapshotSaved_then_offsetsRestored() throws Exception {
        StreamRocketmqP<Map.Entry<Integer, String>> processor = createProcessor(properties(), 2,
                r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(EXACTLY_ONCE));

        rocketmqTestSupport.produce(topic1Name, 0, "0");
        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));

        // create snapshot
        TestInbox snapshot = saveSnapshot(processor, outbox);
        Set<Map.Entry<Object, Object>> snapshotItems = unwrapBroadcastKey(snapshot.queue());

        // consume one more item
        rocketmqTestSupport.produce(topic1Name, 1, "1");
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        // create new processor and restore snapshot
        processor = createProcessor(properties(), 2, r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 10_000);
        outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext().setProcessingGuarantee(EXACTLY_ONCE));

        // restore snapshot
        processor.restoreFromSnapshot(snapshot);
        assertTrue("snapshot not fully processed", snapshot.isEmpty());

        TestInbox snapshot2 = saveSnapshot(processor, outbox);
        assertEquals("new snapshot not equal after restore", snapshotItems, unwrapBroadcastKey(snapshot2.queue()));

        // the second item should be produced one more time
        assertEquals(entry(1, "1"), consumeEventually(processor, outbox));

        assertNoMoreItems(processor, outbox);
    }

    @Test
    public void when_duplicateTopicsProvide_then_uniqueTopicsSubscribed() {
        HazelcastInstance[] instances = instances();
        assertClusterSizeEventually(2, instances);

        // need new topic because we want 2 partitions only
        String topic = randomString();
        rocketmqTestSupport.createTopic(topic, 2);

        Pipeline p = Pipeline.create();
        // Pass the same topic twice
        p.readFrom(RocketmqSources.rocketmq(properties(), topic, topic))
                .withoutTimestamps()
                .setLocalParallelism(1)
                .writeTo(Sinks.list("sink"));

        JobConfig config = new JobConfig();
        Job job = instances[0].getJet().newJob(p, config);

        assertJobStatusEventually(job, JobStatus.RUNNING, 10);

        int messageCount = 1000;
        for (int i = 0; i < messageCount; i++) {
            rocketmqTestSupport.produce(topic, i, Integer.toString(i));
        }

        IList<Object> list = instances[0].getList("sink");
        try {
            // Wait for all messages
            assertTrueEventually(() -> Assertions.assertThat(list).hasSize(messageCount), 15);
            // Check there are no more messages (duplicates..)
            assertTrueAllTheTime(() -> Assertions.assertThat(list).hasSize(messageCount), 1);
        } finally {
            job.cancel();
        }
    }

    private <T> StreamRocketmqP<T> createProcessor(
            Properties properties,
            int numTopics,
            @Nonnull FunctionEx<MessageExt, T> projectionFn,
            long idleTimeoutMillis
    ) {
        assert numTopics == 1 || numTopics == 2;
        ToLongFunctionEx<T> timestampFn = e ->
                e instanceof Map.Entry
                        ? (int) ((Map.Entry) e).getKey()
                        : System.currentTimeMillis();
        EventTimePolicy<T> eventTimePolicy = eventTimePolicy(
                timestampFn, limitingLag(LAG), 1, 0, idleTimeoutMillis);
        List<String> topics = numTopics == 1 ?
                singletonList(topic1Name)
                :
                asList(topic1Name, topic2Name);
        return new StreamRocketmqP<>(properties, topics, projectionFn, eventTimePolicy);
    }

//    @Test
//    public void when_partitionAdded_then_consumedFromBeginning() throws Exception {
//        Properties properties = properties();
//        properties.setProperty("metadata.max.age.ms", "100");
//        StreamRocketmqP processor = createProcessor(properties, 2, entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 10_000);
//        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
//        processor.init(outbox, new TestProcessorContext());
//
//        rocketmqTestSupport.produce(topic1Name, 0, "0");
//        assertEquals(entry(0, "0"), consumeEventually(processor, outbox));
//
//        rocketmqTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 2);
//        rocketmqTestSupport.resetProducer(); // this allows production to the added partition
//
//        boolean somethingInPartition1 = false;
//        for (int i = 1; i < 11; i++) {
//            Future<RecordMetadata> future = rocketmqTestSupport.produce(topic1Name, i, Integer.toString(i));
//            RecordMetadata recordMetadata = future.get();
//            System.out.println("Entry " + i + " produced to partition " + recordMetadata.partition());
//            somethingInPartition1 |= recordMetadata.partition() == 1;
//        }
//        assertTrue("nothing was produced to partition-1", somethingInPartition1);
//        Set<Object> receivedEvents = new HashSet<>();
//        for (int i = 1; i < 11; i++) {
//            try {
//                receivedEvents.add(consumeEventually(processor, outbox));
//            } catch (AssertionError e) {
//                throw new AssertionError("Unable to receive 10 items, events so far: " + receivedEvents);
//            }
//        }
//        assertEquals(range(1, 11).mapToObj(i -> entry(i, Integer.toString(i))).collect(toSet()), receivedEvents);
//    }

//    @Test
//    public void when_partitionAddedWhileJobDown_then_consumedFromBeginning() throws Exception {
//        IList<Map.Entry<Integer, String>> sinkList = instance().getList("sinkList");
//        Pipeline p = Pipeline.create();
//        Properties properties = properties();
//        properties.setProperty("auto.offset.reset", "latest");
//        p.readFrom(RocketmqSources.rocketmq(properties, r->entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), topic1Name))
//                .withoutTimestamps()
//                .writeTo(Sinks.list(sinkList));
//
//        Job job = instance().getJet().newJob(p, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE));
//        assertTrueEventually(() -> {
//            // This might add multiple `0` events to the topic - we need to do this because the source starts from
//            // the latest position and we don't exactly know when it starts, so we try repeatedly
//            rocketmqTestSupport.produce(topic1Name, 0, "0").get();
//            assertFalse(sinkList.isEmpty());
//            assertEquals(entry(0, "0"), sinkList.get(0));
//        });
//        job.suspend();
//        assertJobStatusEventually(job, JobStatus.SUSPENDED);
//        // Note that the job might not have consumed all the zeroes from the topic at this point
//
//        // When
//        rocketmqTestSupport.setPartitionCount(topic1Name, INITIAL_PARTITION_COUNT + 2);
//        // We produce to a partition that didn't exist during the previous job execution.
//        // The job must start reading the new partition from the beginning, otherwise it would miss this item.
//        Map.Entry<Integer, String> event = produceEventToNewPartition(INITIAL_PARTITION_COUNT);
//
//        job.resume();
//        // All events after the resume will be loaded: the non-consumed zeroes, and the possibly multiple
//        // events added in produceEventToNewPartition(). But they must include the event added to the new partition.
//        assertTrueEventually(() -> assertThat(sinkList).contains(event));
//    }

    @Test
    public void when_autoOffsetResetLatest_then_doesNotReadOldMessages() throws InterruptedException {
        IList<Map.Entry<Integer, String>> sinkList = instance().getList("sinkList");
        Pipeline p = Pipeline.create();
        Properties properties = properties();
        properties.setProperty(RocketmqConfig.CONSUMER_OFFSET_RESET_TO, RocketmqConfig.CONSUMER_OFFSET_LATEST);
        p.readFrom(RocketmqSources.rocketmq(properties,
                        r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), topic1Name))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        SendResult result = rocketmqTestSupport.produce(topic1Name, 0, "0");
        assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        instance().getJet().newJob(p);
        // 如果topic还比较新，刚上线不久，新的概念体现在topic第一条消息所在的commitlog还没被清理过，
        // 并且topic的第一个索引文件也没清理过，那么rocketmq会认为这个消息队列消息量不大，可以从头进行消费。
        // assertTrueAllTheTime(() -> assertTrue(sinkList.isEmpty()), 1);
    }

    @Test
    public void when_noAssignedPartitions_thenEmitIdleMsgImmediately() throws Exception {
        StreamRocketmqP<Map.Entry<Integer, String>> processor = createProcessor(properties(), 2, r -> entry(Integer.valueOf(r.getKeys()), new String(r.getBody())), 100_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        TestProcessorContext context = new TestProcessorContext()
                // Set global parallelism to higher number than number of partitions
                .setTotalParallelism(INITIAL_PARTITION_COUNT * 2 + 1)
                .setGlobalProcessorIndex(INITIAL_PARTITION_COUNT * 2);

        processor.init(outbox, context);
        processor.complete();

        assertEquals(IDLE_MESSAGE, outbox.queue(0).poll());
    }

    @Test
    public void when_customProjection_then_used() throws Exception {
        // When
        StreamRocketmqP<String> processor = createProcessor(properties(), 2, r -> r.getKeys() + "=" + new String(r.getBody()), 10_000);
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        rocketmqTestSupport.produce(topic1Name, 0, "0");

        // Then
        assertEquals("0=0", consumeEventually(processor, outbox));
    }

    @Test
    public void when_customProjectionToNull_then_filteredOut() throws Exception {
        // When
        EventTimePolicy<String> eventTimePolicy = eventTimePolicy(
                Long::parseLong,
                limitingLag(0),
                1, 0,
                0
        );
        StreamRocketmqP<String> processor = new StreamRocketmqP<>(
                properties(), singletonList(topic1Name), r -> "0".equals(new String(r.getBody())) ? null : new String(r.getBody()), eventTimePolicy
        );
        TestOutbox outbox = new TestOutbox(new int[]{10}, 10);
        processor.init(outbox, new TestProcessorContext());
        rocketmqTestSupport.produce(topic1Name, 0, "0");
        rocketmqTestSupport.produce(topic1Name, 0, "1");

        // Then
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        }, 3);
        assertEquals("1", outbox.queue(0).poll());
        assertNull(outbox.queue(0).poll());
    }

//    @Test
//    @Ignore("https://github.com/hazelcast/hazelcast-jet/issues/3011")
//    public void when_topicDoesNotExist_then_partitionCountGreaterThanZero() {
//        KafkaConsumer<Integer, String> c = rocketmqTestSupport.createConsumer("non-existing-topic");
//        assertGreaterOrEquals("partition count", c.partitionsFor("non-existing-topic", Duration.ofSeconds(2)).size(), 1);
//    }
//
//    @Test
//    public void when_consumerCannotConnect_then_partitionForTimeout() {
//        Map<String, Object> properties = new HashMap<>();
//        properties.put("bootstrap.servers", "127.0.0.1:33333");
//        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
//        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
//        KafkaConsumer<Integer, String> c = new KafkaConsumer<>(properties);
//        assertThatThrownBy(() -> c.partitionsFor("t", Duration.ofMillis(100)))
//                .isInstanceOf(TimeoutException.class);
//    }

    @SuppressWarnings("unchecked")
    private <T> T consumeEventually(Processor processor, TestOutbox outbox) {
        assertTrueEventually(() -> {
            assertFalse(processor.complete());
            assertFalse("no item in outbox", outbox.queue(0).isEmpty());
        }, 12);
        return (T) outbox.queue(0).poll();
    }

    private void assertNoMoreItems(StreamRocketmqP<?> processor, TestOutbox outbox) throws InterruptedException {
        Thread.sleep(1000);
        assertFalse(processor.complete());
        assertTrue("unexpected items in outbox: " + outbox.queue(0), outbox.queue(0).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private Set<Map.Entry<Object, Object>> unwrapBroadcastKey(Collection<?> c) {
        // BroadcastKey("x") != BroadcastKey("x") ==> we need to extract the key
        Set<Map.Entry<Object, Object>> res = new HashSet<>();
        for (Object o : c) {
            Map.Entry<BroadcastKey<?>, ?> entry = (Map.Entry<BroadcastKey<?>, ?>) o;
            Object equalsSafeValue = entry.getValue() instanceof long[]
                    ? Arrays.toString((long[]) entry.getValue())
                    : entry.getValue();
            res.add(entry(entry.getKey().key(), equalsSafeValue));
        }
        return res;
    }

    private TestInbox saveSnapshot(StreamRocketmqP<?> streamKafkaP, TestOutbox outbox) {
        TestInbox snapshot = new TestInbox();
        assertTrue(streamKafkaP.saveToSnapshot());
        outbox.drainSnapshotQueueAndReset(snapshot.queue(), false);
        return snapshot;
    }

    public static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(RocketmqConfig.NAME_SERVER_ADDR, namesrvAddr);
        properties.setProperty(RocketmqConfig.CONSUMER_OFFSET_RESET_TO, RocketmqConfig.CONSUMER_OFFSET_EARLIEST);
        properties.setProperty(RocketmqConfig.CONSUMER_GROUP, randomString());
        return properties;
    }

    private static Map.Entry<Integer, String> createEntry(int i) {
        return new AbstractMap.SimpleImmutableEntry<>(i, Integer.toString(i));
    }

//    private Map.Entry<Integer, String> produceEventToNewPartition(int partitionId) throws Exception {
//        String value;
//        while (true) {
//            // reset the producer for each attempt as it might not see the new partition yet
//            rocketmqTestSupport.resetProducer();
//            value = UuidUtil.newUnsecureUuidString();
//            Future<RecordMetadata> future = rocketmqTestSupport.produce(topic1Name, partitionId, null, 0, value);
//            RecordMetadata recordMetadata = future.get();
//            if (recordMetadata.partition() == partitionId) {
//                // if the event was added to the correct partition, stop
//                break;
//            }
//            sleepMillis(250);
//        }
//        return entry(0, value);
//    }
}