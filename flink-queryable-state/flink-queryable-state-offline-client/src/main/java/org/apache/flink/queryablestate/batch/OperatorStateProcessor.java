package org.apache.flink.queryablestate.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.queryablestate.batch.mapper.OperatorSubtaskStateMapFunction;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;

public class OperatorStateProcessor<T> {

	final private OperatorState operatorState;

	final private String stateName;

	final private OperatorStateInputFormat inputFormat;

	final private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	final private BackendStateType backendStateType;

	final private StateBackend stateBackend;

	public OperatorStateProcessor(OperatorState operatorState, String stateName,
		  BackendStateType backendStateType, StateBackend stateBackend) {
		this.operatorState = operatorState;
		this.stateName = stateName;
		this.inputFormat = new OperatorStateInputFormat(operatorState);
		this.backendStateType = backendStateType;
		this.stateBackend = stateBackend;
	}

	public DataSet<T> process() {
		DataSource dataSource = env.createInput(inputFormat);
		return dataSource.map(new OperatorSubtaskStateMapFunction(backendStateType, stateBackend));
	}
}
