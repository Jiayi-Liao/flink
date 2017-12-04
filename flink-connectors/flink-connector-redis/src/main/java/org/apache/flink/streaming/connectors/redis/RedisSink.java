package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.Serializable;
import java.util.Properties;

/**
 *  @param <IN> Type of the elements emitted by this sink
 */
public class RedisSink<IN extends Serializable> extends RichSinkFunction<IN>
		implements CheckpointedFunction {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

	private transient Jedis jedis = null;
	private transient Pipeline pipe = null;

	private String host;
	private int port;
	private RedisStore<IN> redisStore;

	public RedisSink(Properties props, RedisStore<IN> store) {
		this.host = props.getProperty("host");
		this.port = Integer.valueOf(props.getProperty("port"));
		this.redisStore = store;

	}

	@Override
	public void open(Configuration parameters) throws Exception {
		jedis = new Jedis(host, port);
		pipe = jedis.pipelined();
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		redisStore.storeInPipe(value, pipe);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

		pipe.syncAndReturnAll();
		if (LOG.isDebugEnabled()) {
			LOG.debug("{} idx {} checkpointed.", getClass().getSimpleName(), subtaskIdx);
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
		LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (jedis != null) {
			jedis.close();
		}
	}
}
