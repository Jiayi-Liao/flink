package org.apache.flink.queryablestate;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.queryablestate.util.OperatorIDUtil;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class SavepointReader {

	private String metadataDirectory;

	private StateBackend stateBackend;

	private SavepointV2 savepoint;

	public SavepointReader(String metadataDirectory, StateBackend stateBackend) {
		this.metadataDirectory = metadataDirectory;
		this.stateBackend = stateBackend;
	}

	public <T> DataSet<T> getAllStates(String uid) {
		OperatorID operatorID = OperatorIDUtil.operatorIDFromUid(uid);
		List<OperatorState> operatorStates = savepoint.getOperatorStates().stream()
			.filter(operatorState -> operatorState.getOperatorID().equals(operatorID)).collect(Collectors.toList());

		if (operatorStates.size() > 1) {
			throw new RuntimeException(String.format("Find more than one operator states for uid: {}", uid));
		}

		if (operatorStates.size() == 0) {
			throw new RuntimeException(String.format("Unable to find operator state for uid: {}", uid));
		}

		OperatorState operatorState = operatorStates.get(0);
		return null;
	}

	private void initializeMetaInfo() throws IOException {
		final CompletedCheckpointStorageLocation checkpointLocation = stateBackend.resolveCheckpoint(metadataDirectory);
		final StreamStateHandle metadataHandle = checkpointLocation.getMetadataHandle();

		Savepoint tempSavepoint;
		try (InputStream in = metadataHandle.openInputStream();
			 DataInputStream dis = new DataInputStream(in)) {

			tempSavepoint = Checkpoints.loadCheckpointMetadata(dis, this.getClass().getClassLoader());

			// only support savepoint v2
			if (!(tempSavepoint instanceof SavepointV2)) {
				throw new UnsupportedOperationException("Only support SavepointV2 now.");
			}
		}

		this.savepoint = (SavepointV2) tempSavepoint;
	}
}
