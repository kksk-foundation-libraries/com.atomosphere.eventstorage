package com.atomosphere.eventstorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.aggregation.EventAggregationStrategy;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;

public class EventAggregator implements Function<Event, List<Event>> {
	private final IgniteCache<Binary, byte[]> eventCache;
	private final IgniteCache<Binary, Integer> registeredVersion;
	private final IgniteCache<Binary, byte[]> snapshotCache;
	private final IgniteCache<Binary, Integer> aggregatedVersion;
	private final EventAggregationStrategy aggregationStrategy;

	public EventAggregator( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, Integer> registeredVersion, //
			IgniteCache<Binary, byte[]> snapshotCache, //
			IgniteCache<Binary, Integer> aggregatedVersion, //
			EventAggregationStrategy aggregationStrategy //
	) {
		this.eventCache = eventCache;
		this.registeredVersion = registeredVersion;
		this.snapshotCache = snapshotCache;
		this.aggregatedVersion = aggregatedVersion;
		this.aggregationStrategy = aggregationStrategy;
	}

	@Override
	public List<Event> apply(Event event) {
		byte[] key = event.getKey();
		List<Event> result = new ArrayList<>();
		Binary binKey = Binary.of(key);
		Lock lock = aggregatedVersion.lock(binKey);
		lock.lock();
		try {
			Integer registered = registeredVersion.get(binKey);
			if (registered == null) {
				registered = 0;
			}
			Integer aggregated = aggregatedVersion.get(binKey);
			if (aggregated == null) {
				aggregated = 0;
			}
			if (aggregated.intValue() < registered.intValue()) {
				result.addAll(aggregationStrategy.aggregate(eventCache, snapshotCache, key, registered, aggregated));
				aggregatedVersion.put(binKey, registered);
			}
		} finally {
			lock.unlock();
		}
		return result;
	}
}
