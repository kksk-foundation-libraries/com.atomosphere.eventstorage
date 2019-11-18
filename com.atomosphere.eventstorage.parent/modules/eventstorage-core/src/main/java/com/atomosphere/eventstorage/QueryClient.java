package com.atomosphere.eventstorage;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.cache.processor.EntryProcessor;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.ColferObject;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.query.QueryCondition;

public class QueryClient<ValueType extends ColferObject> implements Function<QueryCondition, ValueType> {
	private final Constructor<ValueType> constructor;
	private final EventAggregator eventAggregator;
	private final Consumer<List<Event>> aggregatedEventListener;
	private final Function<QueryCondition, ValueType> getFunction;

	public QueryClient(Class<ValueType> valueClass, EventAggregator eventAggregator, Consumer<List<Event>> aggregatedEventListener, IgniteCache<Binary, byte[]> snapshotCache, EntryProcessor<Binary, byte[], byte[]> getEntryProcessor) {
		Constructor<ValueType> _constructor = null;
		try {
			_constructor = valueClass.getConstructor();
		} catch (Exception ignore) {
		}
		this.constructor = _constructor;
		this.eventAggregator = eventAggregator;
		this.aggregatedEventListener = aggregatedEventListener;
		if (getEntryProcessor == null) {
			this.getFunction = queryCondition -> {
				byte[] binValue = snapshotCache.get(Binary.of(queryCondition.getKey()));
				ValueType result = null;
				try {
					result = constructor.newInstance().unmarshal(binValue);
				} catch (Exception ignore) {
				}
				return result;
			};
		} else {
			this.getFunction = queryCondition -> {
				byte[] binValue = snapshotCache.invoke(Binary.of(queryCondition.getKey()), getEntryProcessor, queryCondition.getCondition().marshal());
				ValueType result = null;
				try {
					result = constructor.newInstance().unmarshal(binValue);
				} catch (Exception ignore) {
				}
				return result;
			};
		}
	}

	@Override
	public ValueType apply(QueryCondition queryCondition) {
		aggregatedEventListener.accept(eventAggregator.apply(new Event().withKey(queryCondition.getKey().marshal())));
		return getFunction.apply(queryCondition);
	}
}
