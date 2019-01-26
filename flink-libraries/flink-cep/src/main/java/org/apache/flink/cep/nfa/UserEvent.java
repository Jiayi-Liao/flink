package org.apache.flink.cep.nfa;

public abstract class UserEvent {

	private int userId;

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

}
