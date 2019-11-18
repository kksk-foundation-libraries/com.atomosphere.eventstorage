package com.atomosphere.eventstorage.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;

public class OverwriteStrategy implements EventAggregationStrategy {

	@Override
	public List<Event> aggregate( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, byte[]> snapshotCache, //
			byte[] key, //
			int registered, //
			int aggregated //
	) {
		Event event = new Event().unmarshal(eventCache.get(Binary.of(new EventVersion().withKey(key).withVersion(registered))));
		if (event.getValue() == null) {
			event.setLastValue(snapshotCache.getAndRemove(Binary.of(key)));
		} else {
			event.setLastValue(snapshotCache.getAndPut(Binary.of(key), event.getValue()));
		}
		return new ArrayList<>(Arrays.asList(event));
	}

}
