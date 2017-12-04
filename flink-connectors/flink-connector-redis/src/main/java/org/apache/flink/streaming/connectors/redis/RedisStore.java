package org.apache.flink.streaming.connectors.redis;

import redis.clients.jedis.Pipeline;

import java.io.Serializable;


/**
 *  @param <IN> Type of the elements emitted by this store
 */
public interface RedisStore<IN> extends Serializable {
	void storeInPipe(IN data, Pipeline pipe);
}
