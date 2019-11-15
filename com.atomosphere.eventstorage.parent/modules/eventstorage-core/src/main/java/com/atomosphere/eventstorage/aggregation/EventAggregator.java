package com.atomosphere.eventstorage.aggregation;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.EventStorageHelper;
import com.atomosphere.eventstorage.aggregation.EventAggregationStrategy;
import com.atomosphere.eventstorage.model.Binary;

public class EventAggregator {
	private final IgniteCache<Binary, byte[]> eventCache;
	private final IgniteCache<Binary, Integer> registeredVersion;
	private final IgniteCache<Binary, byte[]> snapshotCache;
	private final IgniteCache<Binary, Integer> aggregatedVersion;
	private final EventAggregationStrategy aggregationStrategy;

	public EventAggregator(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, IgniteCache<Binary, byte[]> snapshotCache, IgniteCache<Binary, Integer> aggregatedVersion, EventAggregationStrategy aggregationStrategy) {
		this.eventCache = eventCache;
		this.registeredVersion = registeredVersion;
		this.snapshotCache = snapshotCache;
		this.aggregatedVersion = aggregatedVersion;
		this.aggregationStrategy = aggregationStrategy;
	}

	public void aggregate(byte[] key) {
		EventStorageHelper.aggregate(eventCache, registeredVersion, snapshotCache, aggregatedVersion, aggregationStrategy, key);
	}
}
