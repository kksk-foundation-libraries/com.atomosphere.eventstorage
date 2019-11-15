package com.atomosphere.eventstorage;

import java.util.Collection;
import java.util.concurrent.locks.Lock;

import javax.cache.processor.EntryProcessor;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.aggregation.EventAggregationStrategy;
import com.atomosphere.eventstorage.entry.processor.EventRegisterEntryProcessor;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;
import com.atomosphere.eventstorage.query.QueryProcessor;

public class EventStorageHelper {

	private EventStorageHelper() {
	}

	private static final EntryProcessor<Binary, Integer, Integer> register = new EventRegisterEntryProcessor();

	public static void register(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, Event event) {
		Binary binKey = Binary.of(event.getKey());
		Lock lock = registeredVersion.lock(binKey);
		try {
			Integer version = registeredVersion.invoke(binKey, register);
			EventVersion eventVersion = new EventVersion().withKey(event.getKey()).withVersion(version);
			eventCache.put(Binary.of(eventVersion), event.withVersion(version).marshal());
		} finally {
			lock.unlock();
		}
	}

	public static void aggregate(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, IgniteCache<Binary, byte[]> snapshotCache, IgniteCache<Binary, Integer> aggregatedVersion, EventAggregationStrategy aggregationStrategy, byte[] key) {
		Binary binKey = Binary.of(key);
		Lock lock = aggregatedVersion.lock(binKey);
		lock.lock();
		try {
			_aggregate(eventCache, registeredVersion, snapshotCache, aggregatedVersion, aggregationStrategy, key, binKey);
		} finally {
			lock.unlock();
		}
	}

	private static void _aggregate(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, IgniteCache<Binary, byte[]> snapshotCache, IgniteCache<Binary, Integer> aggregatedVersion, EventAggregationStrategy aggregationStrategy, byte[] key, Binary binKey) {
		Integer registered = registeredVersion.get(binKey);
		if (registered == null) {
			registered = 0;
		}
		Integer aggregated = aggregatedVersion.get(binKey);
		if (aggregated == null) {
			aggregated = 0;
		}
		if (aggregated.intValue() < registered.intValue()) {
			aggregationStrategy.aggregate(eventCache, snapshotCache, key, registered, aggregated);
			aggregatedVersion.put(binKey, registered);
		}
	}

	public static byte[] query(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, IgniteCache<Binary, byte[]> snapshotCache, IgniteCache<Binary, Integer> aggregatedVersion, EventAggregationStrategy aggregationStrategy, QueryProcessor queryProcessor, byte[] key, byte[] condition) {
		Binary binKey = Binary.of(key);
		Lock lock = aggregatedVersion.lock(binKey);
		lock.lock();
		try {
			_aggregate(eventCache, registeredVersion, snapshotCache, aggregatedVersion, aggregationStrategy, key, binKey);
			return queryProcessor.execute(snapshotCache, key, condition);
		} finally {
			lock.unlock();
		}
	}

	public static void cleanUp(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> removedVersion, Collection<IgniteCache<Binary, Integer>> aggregatedVersions, byte[] key) {
		Binary binKey = Binary.of(key);
		Lock lock = removedVersion.lock(binKey);
		try {
			Integer removed = removedVersion.get(binKey);
			if (removed == null) {
				removed = 0;
			}
			Integer aggregated = 0;
			for (IgniteCache<Binary, Integer> _aggregatedVersion : aggregatedVersions) {
				Integer _aggregated = _aggregatedVersion.get(binKey);
				if (_aggregated != null && _aggregated > aggregated) {
					aggregated = _aggregated;
				}
			}
			for (int i = removed; i < aggregated; i++) {
				eventCache.remove(Binary.of(new EventVersion().withKey(key).withVersion(i)));
			}
			removedVersion.put(binKey, aggregated);
		} finally {
			lock.unlock();
		}
	}
}
