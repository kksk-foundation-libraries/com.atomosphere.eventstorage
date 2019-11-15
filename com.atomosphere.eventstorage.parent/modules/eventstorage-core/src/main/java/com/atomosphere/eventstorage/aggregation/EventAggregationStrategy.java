package com.atomosphere.eventstorage.aggregation;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;

public interface EventAggregationStrategy {
	void aggregate(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, byte[]> snapshotCache, byte[] key, int registered, int aggregated);
}
