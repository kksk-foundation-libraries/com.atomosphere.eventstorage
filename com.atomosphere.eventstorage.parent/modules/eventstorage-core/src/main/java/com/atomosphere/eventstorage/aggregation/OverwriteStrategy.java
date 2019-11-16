package com.atomosphere.eventstorage.aggregation;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;

public class OverwriteStrategy implements EventAggregationStrategy {

	@Override
	public void aggregate( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, byte[]> snapshotCache, //
			byte[] key, //
			int registered, //
			int aggregated //
	) {
		Event event = new Event().unmarshal(eventCache.get(Binary.of(new EventVersion().withKey(key).withVersion(registered))));
		if (event.getValue() == null) {
			snapshotCache.remove(Binary.of(key));
		} else {
			snapshotCache.put(Binary.of(key), event.getValue());
		}
	}

}
