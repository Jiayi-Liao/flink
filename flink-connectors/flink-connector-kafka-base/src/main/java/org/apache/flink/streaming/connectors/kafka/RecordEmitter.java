package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateWithPeriodicWatermarks;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateWithPunctuatedWatermarks;

public class RecordEmitter<T, KPH> {

    // ------------------------------------------------------------------------
    //  emitting records
    // ------------------------------------------------------------------------

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

}
