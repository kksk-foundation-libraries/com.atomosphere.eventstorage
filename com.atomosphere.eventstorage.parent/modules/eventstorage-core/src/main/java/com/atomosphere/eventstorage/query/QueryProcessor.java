package com.atomosphere.eventstorage.query;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;

public interface QueryProcessor {
	byte[] execute( //
			IgniteCache<Binary, byte[]> snapshotCache, //
			byte[] key, //
			byte[] condition //
	);
}
