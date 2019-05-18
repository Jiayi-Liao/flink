package org.apache.flink.queryablestate.batch;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class OperatorStateInputFormat implements InputFormat<OperatorSubtaskState, OperatorStateInputSplit> {

	private OperatorState operatorState;

	private List<OperatorSubtaskState> subtaskStates;

	private int splitIndex;

	private boolean moveToNext;

	public OperatorStateInputFormat(OperatorState operatorState) {
		this.operatorState = operatorState;
		this.subtaskStates = new ArrayList<>(operatorState.getStates());
		this.splitIndex = -1;
		this.moveToNext = false;
	}

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(OperatorStateInputSplit split) throws IOException {
		splitIndex++;
	}

	@Override
	public OperatorStateInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		final int minSplits = Math.max(minNumSplits, subtaskStates.size());

		final List<OperatorStateInputSplit> inputSplits = new ArrayList<>(minSplits);
		for (int i = 0; i < minSplits; i++) {
			inputSplits.add(new OperatorStateInputSplit(i));
		}
		return inputSplits.toArray(new OperatorStateInputSplit[inputSplits.size()]);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(OperatorStateInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return moveToNext;
	}

	@Override
	public OperatorSubtaskState nextRecord(OperatorSubtaskState reuse) throws IOException {
		moveToNext = true;
		return subtaskStates.get(splitIndex);
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}
}
