package com.atomosphere.eventstorage.aggregation;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.ignite.IgniteCache;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.ColferObject;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;

import lombok.AllArgsConstructor;
import lombok.Data;
import reactor.core.publisher.Flux;

public abstract class OverwriteAndChainStrategy<KeyType extends ColferObject, ValueType extends ColferObject> implements EventAggregationStrategy, Publisher<Event> {
	private Consumer<Event> consumer;
	private final Flux<Event> flux;
	private final Function<KeyValuePair<KeyType, ValueType>, byte[]> keyTranslator;
	private final Function<KeyValuePair<KeyType, ValueType>, byte[]> valueTranslator;
	private final Constructor<KeyType> keyConstructor;
	private final Constructor<ValueType> valueConstructor;

	public OverwriteAndChainStrategy( //
			Class<KeyType> keyClass, //
			Class<ValueType> valueClass, //
			Function<KeyValuePair<KeyType, ValueType>, byte[]> keyTranslator, //
			Function<KeyValuePair<KeyType, ValueType>, byte[]> valueTranslator //
	) {
		Constructor<KeyType> _keyConstructor = null;
		try {
			_keyConstructor = keyClass.getConstructor();
		} catch (Exception ignore) {
		}
		keyConstructor = _keyConstructor;
		Constructor<ValueType> _valueConstructor = null;
		try {
			_valueConstructor = valueClass.getConstructor();
		} catch (Exception ignore) {
		}
		valueConstructor = _valueConstructor;
		this.keyTranslator = keyTranslator;
		this.valueTranslator = valueTranslator;
		flux = Flux.create(sink -> {
			consumer = event -> {
				sink.next(event);
			};
			sink.onCancel(() -> {
				consumer = null;
			});
		});
	}

	@Override
	public void aggregate( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, byte[]> snapshotCache, //
			byte[] key, //
			int registered, //
			int aggregated //
	) {
		Event event = new Event().unmarshal(eventCache.get(Binary.of(new EventVersion().withKey(key).withVersion(registered))));
		Event removeEvent = null;
		Event addOrReplaceEvent = null;
		long timestamp = System.currentTimeMillis();
		if (event.getValue() == null) {
			ValueType valueOld = newValue().unmarshal(snapshotCache.getAndRemove(Binary.of(key)));
			byte[] subKeyOld = keyTranslator.apply(new KeyValuePair<>(newKey().unmarshal(key), valueOld));
			removeEvent = new Event().withKey(subKeyOld).withTimestamp(System.currentTimeMillis());
		} else {
			ValueType valueNew = newValue().unmarshal(event.getValue());
			KeyType keyNew = newKey().unmarshal(key);
			byte[] subKeyNew = keyTranslator.apply(new KeyValuePair<>(keyNew, valueNew));
			byte[] subValueNew = valueTranslator.apply(new KeyValuePair<>(keyNew, valueNew));

			byte[] binValueOld = snapshotCache.getAndPut(Binary.of(key), event.getValue());

			if (binValueOld != null) {
				ValueType valueOld = newValue().unmarshal(binValueOld);
				byte[] subKeyOld = keyTranslator.apply(new KeyValuePair<>(keyNew, valueOld));
				if (!Arrays.equals(subKeyNew, subKeyOld)) {
					removeEvent = new Event().withKey(subKeyOld).withTimestamp(timestamp);
				}
			}
			addOrReplaceEvent = new Event().withKey(subKeyNew).withValue(subValueNew).withTimestamp(timestamp);
		}

		if (removeEvent != null) {
			consumer.accept(removeEvent);
		}
		if (addOrReplaceEvent != null) {
			consumer.accept(addOrReplaceEvent);
		}
	}

	@Override
	public void subscribe(Subscriber<? super Event> s) {
		flux.subscribe(s);
	}

	private KeyType newKey() {
		try {
			return keyConstructor.newInstance();
		} catch (Exception ignore) {
			return null;
		}
	}

	private ValueType newValue() {
		try {
			return valueConstructor.newInstance();
		} catch (Exception ignore) {
			return null;
		}
	}

	@Data
	@AllArgsConstructor
	protected static class KeyValuePair<KeyType extends ColferObject, ValueType extends ColferObject> {
		private final KeyType key;
		private final ValueType value;
	}
}
