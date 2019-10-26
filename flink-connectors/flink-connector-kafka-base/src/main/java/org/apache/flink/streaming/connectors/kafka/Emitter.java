package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateWithPeriodicWatermarks;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateWithPunctuatedWatermarks;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.List;

import static org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher.NO_TIMESTAMPS_WATERMARKS;
import static org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher.PERIODIC_WATERMARKS;
import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class Emitter<T, KPH> {

    private final Object checkpointLock;

    private final int timestampWatermarkMode;

    private volatile long maxWatermarkSoFar = Long.MIN_VALUE;

    private final SourceFunction.SourceContext<T> sourceContext;

    private final List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates;

    public Emitter(
            ProcessingTimeService processingTimeProvider,
            SourceFunction.SourceContext<T> sourceContext,
            List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates,
            long autoWatermarkInterval,
            int timestampWatermarkMode) {
        this.sourceContext = sourceContext;
        this.checkpointLock = sourceContext.getCheckpointLock();
        this.timestampWatermarkMode = timestampWatermarkMode;
        this.subscribedPartitionStates = subscribedPartitionStates;
        // if we have periodic watermarks, kick off the interval scheduler
        if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
            @SuppressWarnings("unchecked")
            PeriodicWatermarkEmitter periodicEmitter = new PeriodicWatermarkEmitter(
                    subscribedPartitionStates,
                    sourceContext,
                    processingTimeProvider,
                    autoWatermarkInterval);

            periodicEmitter.start();
        }
    }

    // ------------------------------------------------------------------------
    //  emitting records
    // ------------------------------------------------------------------------

    public abstract void putRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) throws Exception;

    /**
     * Emits a record without attaching an existing timestamp to it.
     *
     * <p>Implementation Note: This method is kept brief to be JIT inlining friendly.
     * That makes the fast path efficient, the extended paths are called as separate methods.
     *
     * @param record The record to emit
     * @param partitionState The state of the Kafka partition from which the record was fetched
     * @param offset The offset of the record
     */
    protected void emitRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) throws Exception {
        if (record != null) {
            if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
                // fast path logic, in case there are no watermarks

                // emit the record, using the checkpoint lock to guarantee
                // atomicity of record emission and offset state update
                synchronized (checkpointLock) {
                    sourceContext.collect(record);
                    partitionState.setOffset(offset);
                }
            } else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
                emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, Long.MIN_VALUE);
            } else {
                emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, Long.MIN_VALUE);
            }
        } else {
            // if the record is null, simply just update the offset state for partition
            synchronized (checkpointLock) {
                partitionState.setOffset(offset);
            }
        }
    }

    /**
     * Emits a record attaching a timestamp to it.
     *
     * <p>Implementation Note: This method is kept brief to be JIT inlining friendly.
     * That makes the fast path efficient, the extended paths are called as separate methods.
     *
     * @param record The record to emit
     * @param partitionState The state of the Kafka partition from which the record was fetched
     * @param offset The offset of the record
     */
    protected void emitRecordWithTimestamp(
            T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long timestamp) throws Exception {

        if (record != null) {
            if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
                // fast path logic, in case there are no watermarks generated in the fetcher

                // emit the record, using the checkpoint lock to guarantee
                // atomicity of record emission and offset state update
                synchronized (checkpointLock) {
                    sourceContext.collectWithTimestamp(record, timestamp);
                    partitionState.setOffset(offset);
                }
            } else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
                emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, timestamp);
            } else {
                emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, timestamp);
            }
        } else {
            // if the record is null, simply just update the offset state for partition
            synchronized (checkpointLock) {
                partitionState.setOffset(offset);
            }
        }
    }

    /**
     * Record emission, if a timestamp will be attached from an assigner that is
     * also a periodic watermark generator.
     */
    private void emitRecordWithTimestampAndPeriodicWatermark(
            T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long kafkaEventTimestamp) {
        @SuppressWarnings("unchecked")
        final KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH> withWatermarksState =
                (KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH>) partitionState;

        // extract timestamp - this accesses/modifies the per-partition state inside the
        // watermark generator instance, so we need to lock the access on the
        // partition state. concurrent access can happen from the periodic emitter
        final long timestamp;
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (withWatermarksState) {
            timestamp = withWatermarksState.getTimestampForRecord(record, kafkaEventTimestamp);
        }

        // emit the record with timestamp, using the usual checkpoint lock to guarantee
        // atomicity of record emission and offset state update
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, timestamp);
            partitionState.setOffset(offset);
        }
    }

    /**
     * Record emission, if a timestamp will be attached from an assigner that is
     * also a punctuated watermark generator.
     */
    private void emitRecordWithTimestampAndPunctuatedWatermark(
            T record, KafkaTopicPartitionState<KPH> partitionState, long offset, long kafkaEventTimestamp) {
        @SuppressWarnings("unchecked")
        final KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH> withWatermarksState =
                (KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>) partitionState;

        // only one thread ever works on accessing timestamps and watermarks
        // from the punctuated extractor
        final long timestamp = withWatermarksState.getTimestampForRecord(record, kafkaEventTimestamp);
        final Watermark newWatermark = withWatermarksState.checkAndGetNewWatermark(record, timestamp);

        // emit the record with timestamp, using the usual checkpoint lock to guarantee
        // atomicity of record emission and offset state update
        synchronized (checkpointLock) {
            sourceContext.collectWithTimestamp(record, timestamp);
            partitionState.setOffset(offset);
        }

        // if we also have a new per-partition watermark, check if that is also a
        // new cross-partition watermark
        if (newWatermark != null) {
            updateMinPunctuatedWatermark(newWatermark);
        }
    }
    /**
     * Checks whether a new per-partition watermark is also a new cross-partition watermark.
     */
    private void updateMinPunctuatedWatermark(Watermark nextWatermark) {
        if (nextWatermark.getTimestamp() > maxWatermarkSoFar) {
            long newMin = Long.MAX_VALUE;

            for (KafkaTopicPartitionState<?> state : subscribedPartitionStates) {
                @SuppressWarnings("unchecked")
                final KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH> withWatermarksState =
                        (KafkaTopicPartitionStateWithPunctuatedWatermarks<T, KPH>) state;

                newMin = Math.min(newMin, withWatermarksState.getCurrentPartitionWatermark());
            }

            // double-check locking pattern
            if (newMin > maxWatermarkSoFar) {
                synchronized (checkpointLock) {
                    if (newMin > maxWatermarkSoFar) {
                        maxWatermarkSoFar = newMin;
                        sourceContext.emitWatermark(new Watermark(newMin));
                    }
                }
            }
        }
    }

    /**
     * The periodic watermark emitter. In its given interval, it checks all partitions for
     * the current event time watermark, and possibly emits the next watermark.
     */
    private static class PeriodicWatermarkEmitter<KPH> implements ProcessingTimeCallback {

        private final List<KafkaTopicPartitionState<KPH>> allPartitions;

        private final SourceFunction.SourceContext<?> emitter;

        private final ProcessingTimeService timerService;

        private final long interval;

        private long lastWatermarkTimestamp;

        //-------------------------------------------------

        PeriodicWatermarkEmitter(
                List<KafkaTopicPartitionState<KPH>> allPartitions,
                SourceFunction.SourceContext<?> emitter,
                ProcessingTimeService timerService,
                long autoWatermarkInterval) {
            this.allPartitions = checkNotNull(allPartitions);
            this.emitter = checkNotNull(emitter);
            this.timerService = checkNotNull(timerService);
            this.interval = autoWatermarkInterval;
            this.lastWatermarkTimestamp = Long.MIN_VALUE;
        }

        //-------------------------------------------------

        public void start() {
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }

        @Override
        public void onProcessingTime(long timestamp) throws Exception {

            long minAcrossAll = Long.MAX_VALUE;
            boolean isEffectiveMinAggregation = false;
            for (KafkaTopicPartitionState<?> state : allPartitions) {

                // we access the current watermark for the periodic assigners under the state
                // lock, to prevent concurrent modification to any internal variables
                final long curr;
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (state) {
                    curr = ((KafkaTopicPartitionStateWithPeriodicWatermarks<?, ?>) state).getCurrentWatermarkTimestamp();
                }

                minAcrossAll = Math.min(minAcrossAll, curr);
                isEffectiveMinAggregation = true;
            }

            // emit next watermark, if there is one
            if (isEffectiveMinAggregation && minAcrossAll > lastWatermarkTimestamp) {
                lastWatermarkTimestamp = minAcrossAll;
                emitter.emitWatermark(new Watermark(minAcrossAll));
            }

            // schedule the next watermark
            timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
        }
    }
}
