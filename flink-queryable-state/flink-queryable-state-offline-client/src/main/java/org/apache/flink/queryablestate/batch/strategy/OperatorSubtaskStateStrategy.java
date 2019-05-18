package org.apache.flink.queryablestate.batch.strategy;

public interface OperatorSubtaskStateStrategy {

	void open();

	void read();

	void close();
}
