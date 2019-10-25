package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;

public abstract class TimestampSyncEmitter<T, KPH> {


    public void putRecord(T record, KafkaTopicPartitionState<KPH> partitionState, long offset) {

    }
}
