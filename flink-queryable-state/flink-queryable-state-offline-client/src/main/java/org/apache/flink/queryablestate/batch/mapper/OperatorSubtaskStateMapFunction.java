package org.apache.flink.queryablestate.batch.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.batch.strategy.OperatorSubtaskStateStrategy;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

public class OperatorSubtaskStateMapFunction<T> extends RichMapFunction<OperatorSubtaskState, T> {

	final private StateMetaInfoSnapshot.BackendStateType backendStateType;

	final private StateBackend stateBackend;

	final private OperatorSubtaskStateStrategy strategy;

	public OperatorSubtaskStateMapFunction(StateMetaInfoSnapshot.BackendStateType backendStateType,
										   StateBackend stateBackend) {
		this.backendStateType = backendStateType;
		this.stateBackend = stateBackend;
	}

	@Override
	public void open(Configuration parameters) throws Exception {

	}

	@Override
	public T map(OperatorSubtaskState subtaskState) throws Exception {

		return null;
	}
}
