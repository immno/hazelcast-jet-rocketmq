package io.github.immno.jet.rocketmq.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.*;
import com.hazelcast.jet.impl.util.LoggingUtil;
import io.github.immno.jet.rocketmq.RocketmqConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StreamRocketmqP<T> extends AbstractProcessor {

    /**
     * RocketMQ itself supports multi-threaded consumption, so it is set to 1.
     */
    public static final int PREFERRED_LOCAL_PARALLELISM = 2;
    private static final long METADATA_CHECK_INTERVAL_NANOS = SECONDS.toNanos(5);
    private static final String MESSAGE_QUEUE_SNAPSHOT_KEY = "messageQueues";

    /**
     * MessageQueue processed by the current processor
     */
    Map<MessageQueue, Integer> currentAssignment = new HashMap<>();

    private final Properties properties;
    private final FunctionEx<? super MessageExt, ? extends T> projectionFn;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private List<String> topics;
    private int totalParallelism;

    private DefaultLitePullConsumer consumer;
    private long nextMetadataCheck = Long.MIN_VALUE;

    /**
     * Key: MessageQueue<br>
     * Value: partition offsets, at index I is offset for partition I.<br>
     * Offsets are -1 initially and remain -1 for partitions not assigned to this
     * processor.
     */
    private final Map<MessageQueue, Long> offsets = new HashMap<>();
    private Traverser<Map.Entry<BroadcastKey<?>, ?>> snapshotTraverser;
    private int processorIndex;
    private Traverser<Object> traverser = Traversers.empty();

    public StreamRocketmqP(
            @Nonnull Properties properties,
            @Nonnull List<String> topics,
            @Nonnull FunctionEx<? super MessageExt, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        this.properties = properties;
        this.topics = topics;
        this.projectionFn = projectionFn;
        eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        List<String> uniqueTopics = this.topics.stream().distinct().collect(Collectors.toList());
        if (uniqueTopics.size() != this.topics.size()) {
            List<String> topics = new ArrayList<>(this.topics);
            for (String t : uniqueTopics) {
                topics.remove(t);
            }
            getLogger().warning("Duplicate topics found in topic list: " + topics);
        }
        this.topics = uniqueTopics;
        this.processorIndex = context.globalProcessorIndex();
        this.totalParallelism = context.totalParallelism();

        String group = properties.getProperty(RocketmqConfig.CONSUMER_GROUP, "JET_ROCKETMQ_GROUP");

        this.consumer = new DefaultLitePullConsumer(group);
        RocketmqConfig.buildConsumerConfigs(properties, this.consumer);
        this.consumer.setAutoCommit(true);
        this.consumer.setInstanceName("Jet-" + processorIndex);
        // TODO How to determine the number of threads to consume
        this.consumer.setPullThreadNums(2);
        try {
            this.consumer.start();
        } catch (MQClientException e) {
            getLogger().warning("Unable to start consumer", e);
        }
    }

    private void assignMessageQueue() {
        if (System.nanoTime() < this.nextMetadataCheck) {
            return;
        }
        for (int topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
            Collection<MessageQueue> newMessageQueue;
            String topicName = this.topics.get(topicIndex);
            try {
                newMessageQueue = consumer.fetchMessageQueues(topicName);
            } catch (MQClientException e) {
                getLogger().warning("Unable to get queue metadata, ignoring: " + e, e);
                return;
            }

            handleNewMessageQueue(topicIndex, newMessageQueue, false);
        }

        this.nextMetadataCheck = System.nanoTime() + METADATA_CHECK_INTERVAL_NANOS;
    }

    private void handleNewMessageQueue(int topicIndex, Collection<MessageQueue> messageQueue,
                                       boolean isSnapshotRecovery) {
        Collection<MessageQueue> messageQueueList = new ArrayList<>(messageQueue);
        String topicName = this.topics.get(topicIndex);
        Map<MessageQueue, Long> oldTopicOffsets = topicOffsets(topicName);
        if (oldTopicOffsets.size() >= messageQueueList.size()) {
            return;
        }
        // extend the offsets array for this topic
        messageQueueList.removeAll(oldTopicOffsets.keySet());
        for (MessageQueue queue : messageQueueList) {
            this.offsets.put(queue, -1L);
        }

        Collection<MessageQueue> newAssignments = new ArrayList<>();
        for (MessageQueue newAssignment : messageQueueList) {
            if (handledByThisProcessor(topicIndex, newAssignment.getQueueId())) {
                currentAssignment.put(newAssignment, currentAssignment.size());
                newAssignments.add(newAssignment);
            }
        }
        if (newAssignments.isEmpty()) {
            return;
        }
        getLogger().info("New messagequeue(s) assigned: " + newAssignments);
        eventTimeMapper.addPartitions(newAssignments.size());
        consumer.assign(currentAssignment.keySet());
        if (!oldTopicOffsets.isEmpty() && !isSnapshotRecovery) {
            // For partitions detected later during the runtime we seek to their
            // beginning. It can happen that a partition is added, and some messages
            // are added to it before we start consuming from it. If we started at the
            // current position, we will miss those, so we explicitly seek to the
            // beginning.
            getLogger().info("Seeking to the beginning of newly-discovered queue: " + newAssignments);
            for (MessageQueue assignment : newAssignments) {
                try {
                    consumer.seekToBegin(assignment);
                } catch (MQClientException e) {
                    getLogger().warning("Unable to seekToBegin, ignoring: " + assignment, e);
                }
            }
        }
        LoggingUtil.logFinest(getLogger(), "Currently assigned queue: %s", currentAssignment);
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        List<MessageExt> records = null;
        assignMessageQueue();
        if (!currentAssignment.isEmpty()) {
            records = consumer.poll(0);
        }

        traverser = isEmpty(records)
                ? eventTimeMapper.flatMapIdle()
                : traverseIterable(records).flatMap(record -> {
            MessageQueue mq = new MessageQueue(record.getTopic(), record.getBrokerName(), record.getQueueId());
            this.offsets.put(mq, record.getQueueOffset());
            T projectedRecord = projectionFn.apply(record);
            if (projectedRecord == null) {
                return Traversers.empty();
            }
            return eventTimeMapper.flatMapEvent(projectedRecord, currentAssignment.get(mq),
                    record.getBornTimestamp());
        });

        emitFromTraverser(traverser);
        return false;
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            Stream<Map.Entry<BroadcastKey<?>, ?>> snapshotStream = offsets.entrySet().stream()
                    .filter(mapper -> mapper.getValue() >= 0)
                    .map(entry -> {
                        MessageQueue key = entry.getKey();
                        long offset = entry.getValue();
                        long watermark = eventTimeMapper.getWatermark(currentAssignment.get(key));
                        return entry(broadcastKey(key), new long[]{offset, watermark});
                    });
            snapshotTraverser = traverseStream(snapshotStream)
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        LoggingUtil.logFinest(getLogger(),
                                "Finished saving snapshot.Saved offsets: %s, Saved watermarks: %s",
                                offsets(), watermarks());
                    });

            if (processorIndex == 0) {
                Map.Entry<BroadcastKey<?>, ?> partitionCountsItem = entry(
                        broadcastKey(MESSAGE_QUEUE_SNAPSHOT_KEY), new HashMap<>(this.offsets));
                snapshotTraverser = snapshotTraverser.append(partitionCountsItem);
            }
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key0, @Nonnull Object value) {
        @SuppressWarnings("unchecked")
        Object key = ((BroadcastKey<Object>) key0).key();
        if (MESSAGE_QUEUE_SNAPSHOT_KEY.equals(key)) {
            @SuppressWarnings("unchecked")
            Map<MessageQueue, Long> messageQueue = (Map<MessageQueue, Long>) value;
            restorebyKey(messageQueue);
        } else {
            MessageQueue mq = (MessageQueue) key;
            long[] value1 = (long[]) value;
            long offset = value1[0];
            long watermark = value1[1];

            String topic = mq.getTopic();
            if (topicOffsets(topic).isEmpty()) {
                getLogger().warning("Offset for topic '" + topic
                        + "' is restored from the snapshot, but the topic is not supposed to be read, ignoring");
                return;
            }
            int topicIndex = this.topics.indexOf(topic);
            assert topicIndex >= 0;
            handleNewMessageQueue(topicIndex, Collections.singletonList(mq), true);
            if (!handledByThisProcessor(topicIndex, mq.getQueueId())) {
                return;
            }
            long mqOffset = this.offsets.get(mq);
            assert mqOffset < 0
                    : "duplicate offset for topicPartition '" + mq
                    + "' restored, offset1=" + mqOffset + ", offset2=" + offset;
            this.offsets.put(mq, offset);
            try {
                consumer.seek(mq, offset + 1);
            } catch (MQClientException e) {
                getLogger().warning("Unable to seekTo " + (offset + 1) + ", ignoring: " + mq, e);
            }
            Integer partitionIndex = currentAssignment.get(mq);
            assert partitionIndex != null;
            eventTimeMapper.restoreWatermark(partitionIndex, watermark);
        }
    }

    private void restorebyKey(Map<MessageQueue, Long> messageQueue) {
        Map<String, Set<MessageQueue>> topicQueueMap = messageQueue.entrySet().stream()
                .collect(Collectors.groupingBy(keyMapper -> keyMapper.getKey().getTopic(),
                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));

        for (Map.Entry<String, Set<MessageQueue>> topicQueueEntry : topicQueueMap.entrySet()) {
            String topicName = topicQueueEntry.getKey();
            Set<MessageQueue> mq = topicQueueEntry.getValue();
            int topicIndex = topics.indexOf(topicName);
            assert topicIndex >= 0;
            handleNewMessageQueue(topicIndex, mq, true);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        LoggingUtil.logFinest(getLogger(),
                "Finished restoring snapshot. Restored offsets: %s, and watermarks: %s",
                offsets(), watermarks());
        return true;
    }

    private boolean isEmpty(List<MessageExt> records) {
        return records == null || records.isEmpty();
    }

    private Map<MessageQueue, Long> topicOffsets(String topicName) {
        Map<MessageQueue, Long> topicOffsets = new HashMap<>(this.offsets);
        topicOffsets.entrySet().removeIf(entry -> !Objects.equals(entry.getKey().getTopic(), topicName));
        return topicOffsets;
    }

    private Map<MessageQueue, Long> offsets() {
        return this.currentAssignment.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, this.offsets::get));
    }

    private Map<MessageQueue, Long> watermarks() {
        return this.currentAssignment.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> this.eventTimeMapper.getWatermark(e.getValue())));
    }

    @Nonnull
    public static <T> SupplierEx<Processor> processorSupplier(
            @Nonnull Properties properties,
            @Nonnull List<String> topics,
            @Nonnull FunctionEx<? super MessageExt, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        return () -> new StreamRocketmqP<>(properties, topics, projectionFn, eventTimePolicy);
    }

    private boolean handledByThisProcessor(int topicIndex, int queue) {
        return handledByThisProcessor(this.totalParallelism, this.offsets.size(), this.processorIndex, topicIndex,
                queue);
    }

    static boolean handledByThisProcessor(
            int totalParallelism, int topicsCount, int processorIndex, int topicIndex, int partition) {
        int startIndex = topicIndex * Math.max(1, totalParallelism / topicsCount);
        int topicPartitionHandledBy = (startIndex + partition) % totalParallelism;
        return topicPartitionHandledBy == processorIndex;
    }
}