package com.atomosphere.eventstorage.query;

import java.lang.reflect.Constructor;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.aggregation.EventAggregationStrategy;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.ColferObject;

public class QueryClient<ValueType extends ColferObject> implements Function<QueryCondition, ValueType> {
	private static final QueryProcessor DEFAULT_QUERY_PROCESSOR = new SimpleQueryProcessor();
	private final Constructor<ValueType> constructor;
	private final QueryProcessor queryProcessor;
	private final IgniteCache<Binary, byte[]> eventCache;
	private final IgniteCache<Binary, Integer> registeredVersion;
	private final IgniteCache<Binary, byte[]> snapshotCache;
	private final IgniteCache<Binary, Integer> aggregatedVersion;
	private final EventAggregationStrategy aggregationStrategy;

	public QueryClient(Class<ValueType> valueClass, IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, IgniteCache<Binary, byte[]> snapshotCache, IgniteCache<Binary, Integer> aggregatedVersion, EventAggregationStrategy aggregationStrategy) {
		this(valueClass, eventCache, registeredVersion, snapshotCache, aggregatedVersion, aggregationStrategy, DEFAULT_QUERY_PROCESSOR);
	}

	public QueryClient(Class<ValueType> valueClass, IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, IgniteCache<Binary, byte[]> snapshotCache, IgniteCache<Binary, Integer> aggregatedVersion, EventAggregationStrategy aggregationStrategy, QueryProcessor queryProcessor) {
		Constructor<ValueType> _constructor = null;
		try {
			_constructor = valueClass.getConstructor();
		} catch (Exception ignore) {
		}
		constructor = _constructor;
		this.queryProcessor = queryProcessor;
		this.eventCache = eventCache;
		this.registeredVersion = registeredVersion;
		this.snapshotCache = snapshotCache;
		this.aggregatedVersion = aggregatedVersion;
		this.aggregationStrategy = aggregationStrategy;
	}

	@Override
	public ValueType apply(QueryCondition condition) {
		byte[] data = query(condition.getKey().marshal(), condition.getCondition().marshal());
		return newValue().unmarshal(data);
	}

	private byte[] query(byte[] key, byte[] condition) {
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
				aggregationStrategy.aggregate(eventCache, snapshotCache, key, registered, aggregated);
				aggregatedVersion.put(binKey, registered);
			}
			return queryProcessor.execute(snapshotCache, key, condition);
		} finally {
			lock.unlock();
		}
	}

	private ValueType newValue() {
		try {
			return constructor.newInstance();
		} catch (Exception ignore) {
			return null;
		}
	}
}
