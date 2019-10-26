package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.Handover;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.List;
import java.util.Map;
import java.util.Queue;

public class TimestampSyncEmitter<T, KPH> extends Emitter<T, KPH> {

    private final Map<KafkaTopicPartitionState<KPH>, Queue<T>> bufferedRecords;

    private final long MAX_LOOKAHEAD_MILLIS;

    private final long minWatermark;

    private final long maxBufferLenght;

    private final Handover handover;

    public TimestampSyncEmitter(ProcessingTimeService processingTimeProvider,
                                      SourceFunction.SourceContext<T> sourceContext,
                                      List<KafkaTopicPartitionState<KPH>> subscribedPartitionStates,
                                      long autoWatermarkInterval,
                                      int timestampWatermarkMode,
                                      Handover handover) {
        super(processingTimeProvider, sourceContext, subscribedPartitionStates, autoWatermarkInterval, timestampWatermarkMode);
        this.handover = handover;
    }

    @Override
    public void putRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) throws Exception {

    }
}
