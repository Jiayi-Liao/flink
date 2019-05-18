package org.apache.flink.queryablestate.batch;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.checkpoint.OperatorState;

public class OperatorStateInputSplit implements InputSplit {

	private int splitNum;

	public OperatorStateInputSplit(int splitNum) {
		this.splitNum = splitNum;
	}

	@Override
	public int getSplitNumber() {
		return this.splitNum;
	}
}
