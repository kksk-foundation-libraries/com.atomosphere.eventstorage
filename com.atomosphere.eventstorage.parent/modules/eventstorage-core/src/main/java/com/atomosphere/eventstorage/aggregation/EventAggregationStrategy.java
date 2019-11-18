package com.atomosphere.eventstorage.aggregation;

import java.util.List;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;

public interface EventAggregationStrategy {
	List<Event> aggregate( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, byte[]> snapshotCache, //
			byte[] key, //
			int registered, //
			int aggregated //
	);
}
