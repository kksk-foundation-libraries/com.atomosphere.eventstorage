package com.atomosphere.eventstorage.query;

import java.lang.reflect.Constructor;
import java.util.function.Function;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.EventStorageHelper;
import com.atomosphere.eventstorage.aggregation.EventAggregationStrategy;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.ColferObject;
import com.atomosphere.eventstorage.query.QueryProcessor;
import com.atomosphere.eventstorage.query.SimpleQueryProcessor;

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
		byte[] data = EventStorageHelper.query(eventCache, registeredVersion, snapshotCache, aggregatedVersion, aggregationStrategy, queryProcessor, condition.getKey().marshal(), condition.getCondition().marshal());
		return newValue().unmarshal(data);
	}

	private ValueType newValue() {
		try {
			return constructor.newInstance();
		} catch (Exception ignore) {
			return null;
		}
	}
}
