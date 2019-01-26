/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.operator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.UserEvent;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Abstract CEP pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state. Additionally, the set of all seen keys is kept as part of the
 * operator state. This is necessary to trigger the execution for all keys upon receiving a new
 * watermark.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
public abstract class AbstractKeyedCEPPatternOperator<IN extends UserEvent, KEY, OUT, F extends Function>
		extends AbstractUdfStreamOperator<OUT, F>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = -4166778210774160757L;

	private final TypeSerializer<IN> inputSerializer;

	///////////////			State			//////////////

	private static final String NFA_STATE_NAME = "nfaStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

	private final NFACompiler.NFAFactory<IN> nfaFactory;

	private transient ValueState<NFAState> computationStates;
	private transient MapState<Long, List<IN>> elementQueueState;
	private transient SharedBuffer<IN> partialMatches;

	private transient InternalTimerService<VoidNamespace> timerService;

	private transient NFA<IN> nfa;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	protected final OutputTag<IN> lateDataOutputTag;

	protected final AfterMatchSkipStrategy afterMatchSkipStrategy;

	public AbstractKeyedCEPPatternOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			final NFACompiler.NFAFactory<IN> nfaFactory,
			final EventComparator<IN> comparator,
			final AfterMatchSkipStrategy afterMatchSkipStrategy,
			final F function,
			final OutputTag<IN> lateDataOutputTag) {
		super(function);

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);
		this.lateDataOutputTag = lateDataOutputTag;

		if (afterMatchSkipStrategy == null) {
			this.afterMatchSkipStrategy = AfterMatchSkipStrategy.noSkip();
		} else {
			this.afterMatchSkipStrategy = afterMatchSkipStrategy;
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// initializeState through the provided context
		computationStates = context.getKeyedStateStore().getState(
				new ValueStateDescriptor<>(
						NFA_STATE_NAME,
						NFAStateSerializer.INSTANCE));

		partialMatches = new SharedBuffer<>(context.getKeyedStateStore(), inputSerializer);

		elementQueueState = context.getKeyedStateStore().getMapState(
				new MapStateDescriptor<>(
						EVENT_QUEUE_STATE_NAME,
						LongSerializer.INSTANCE,
						new ListSerializer<>(inputSerializer)));
	}

	@Override
	public void open() throws Exception {
		super.open();

		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);

		this.nfa = nfaFactory.createNFA();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// there can be no out of order elements in processing time
		NFAState nfaState = getNFAState();
		long timestamp = getProcessingTimeService().getCurrentProcessingTime();

		// advanceTime(nfaState, timestamp);  We don't need it for now
		processEvent(nfaState, element.getValue(), timestamp);
		updateNFA(nfaState);
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) {
		// do not support event time
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// do not support processing time timer
	}

	private NFAState getNFAState() throws IOException {
		NFAState nfaState = computationStates.value();
		return nfaState != null ? nfaState : nfa.createInitialNFAState();
	}

	private void updateNFA(NFAState nfaState) throws IOException {
		if (nfaState.isStateChanged()) {
			nfaState.resetStateChanged();
			computationStates.update(nfaState);
		}
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfaState Our NFAState object
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	private void processEvent(NFAState nfaState, IN event, long timestamp) throws Exception {
		Collection<Map<String, List<IN>>> patterns =
				nfa.process(partialMatches, nfaState, event, timestamp, afterMatchSkipStrategy);
		processMatchedSequences(patterns, timestamp);
	}

	protected abstract void processMatchedSequences(Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception;

	//////////////////////			Testing Methods			//////////////////////

	@VisibleForTesting
	public boolean hasNonEmptySharedBuffer(KEY key) throws Exception {
		setCurrentKey(key);
		return !partialMatches.isEmpty();
	}

	@VisibleForTesting
	public boolean hasNonEmptyPQ(KEY key) throws Exception {
		setCurrentKey(key);
		return elementQueueState.keys().iterator().hasNext();
	}

	@VisibleForTesting
	public int getPQSize(KEY key) throws Exception {
		setCurrentKey(key);
		int counter = 0;
		for (List<IN> elements: elementQueueState.values()) {
			counter += elements.size();
		}
		return counter;
	}
}
