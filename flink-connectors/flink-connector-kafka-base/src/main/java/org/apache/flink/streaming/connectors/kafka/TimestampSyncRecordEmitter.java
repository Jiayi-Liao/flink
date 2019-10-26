package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.List;

public class TimestampSyncRecordEmitter<T, KPH> extends RecordEmitter<T, KPH> {

    public TimestampSyncRecordEmitter(ProcessingTimeService processingTimeProvider,
                                      SourceFunction.SourceContext<T> sourceContext,
                                      List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates,
                                      long autoWatermarkInterval,
                                      int timestampWatermarkMode) {
        super(processingTimeProvider, sourceContext, subscribedPartitionStates, autoWatermarkInterval, timestampWatermarkMode);
    }

    @Override
    public void putRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) throws Exception {

    }
}
