package com.atomosphere.eventstorage.aggregation;

import java.util.function.Consumer;

import org.apache.ignite.IgniteCache;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;
import com.atomosphere.eventstorage.model.colfer.UpdateEvent;

import reactor.core.publisher.Flux;

public class OverwriteAndChainStrategy implements EventAggregationStrategy, Publisher<UpdateEvent> {
	private Consumer<UpdateEvent> consumer;
	private final Flux<UpdateEvent> flux;

	public OverwriteAndChainStrategy() {
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
		UpdateEvent updateEvent = new UpdateEvent() //
				.withKey(event.getKey()) //
				.withNewValue(event.getValue()) //
				.withTimestamp(event.getTimestamp()) //
				.withVersion(event.getVersion());
		if (event.getValue() == null) {
			updateEvent.setOldValue(snapshotCache.getAndRemove(Binary.of(key)));
		} else {
			updateEvent.setOldValue(snapshotCache.getAndPut(Binary.of(key), event.getValue()));
		}
		consumer.accept(updateEvent);
	}

	@Override
	public void subscribe(Subscriber<? super UpdateEvent> s) {
		flux.subscribe(s);
	}

}
