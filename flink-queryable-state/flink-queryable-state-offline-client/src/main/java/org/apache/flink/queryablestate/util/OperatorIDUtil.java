package org.apache.flink.queryablestate.util;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import java.nio.charset.Charset;

public class OperatorIDUtil {

	private static final HashFunction hashFunction = Hashing.murmur3_128(0);

	public static OperatorID operatorIDFromUid(String uid) {
		Hasher hasher = hashFunction.newHasher();
		hasher.putString(uid, Charset.forName("UTF-8"));
		return new OperatorID(hasher.hash().asBytes());
	}
}
