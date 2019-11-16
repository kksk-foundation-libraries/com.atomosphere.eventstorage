package com.atomosphere.eventstorage.management;

import java.util.Collection;
import java.util.concurrent.locks.Lock;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.EventVersion;

public class EventStorageManager {
	public static void cleanUp( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, Integer> removedVersion, //
			Collection<IgniteCache<Binary, Integer>> aggregatedVersions, //
			byte[] key //
	) {
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
