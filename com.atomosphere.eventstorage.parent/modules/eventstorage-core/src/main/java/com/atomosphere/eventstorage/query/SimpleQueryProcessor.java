package com.atomosphere.eventstorage.query;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;

public class SimpleQueryProcessor implements QueryProcessor {

	@Override
	public byte[] execute( //
			IgniteCache<Binary, byte[]> snapshotCache, //
			byte[] key, //
			byte[] condition //
	) {
		return snapshotCache.get(Binary.of(key));
	}

}
