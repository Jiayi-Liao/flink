package org.apache.flink.streaming.connectors.redis.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.RedisStore;

import redis.clients.jedis.Pipeline;

import java.util.Properties;

/**
 * This is an example which uses redis sink to store a key-value structure data.
 */
public class RedisSinkExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> source = env.generateSequence(0, 10).map(new MapFunction<Long, String>() {
			@Override
			public String map(Long value) throws Exception {
				return String.join(".", String.valueOf(value), String.valueOf(value * 2));
			}
		});
		Properties props = new Properties();
		props.setProperty("host", "127.0.0.1");
		props.setProperty("port", "6379");
		source.addSink(new RedisSink<>(props, new RedisStore<String>() {
			@Override
			public void storeInPipe(String data, Pipeline pipe) {
				if (data.split(".").length == 2) {
					String key = data.split(".")[0];
					String value = data.split(".")[1];
					pipe.set(key, value);
				}
			}
		}));
		env.execute("Redis Sink Example");
	}
}
