package com.atomosphere.eventstorage.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.EventStorageHelper;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;

import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class CommandEventProcessor implements Consumer<Event> {
	private final AtomicReference<Consumer<Event>> consumer = new AtomicReference<>(null);
	private final Disposable disposable;

	public CommandEventProcessor(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion, List<Consumer<Event>> subProcessors) {
		ConnectableFlux<Event> flux = Flux.<Event>create(sink -> {
			consumer.set(event -> {
				sink.next(event);
			});
			sink.onCancel(() -> {
				consumer.set(null);
			});
		}, FluxSink.OverflowStrategy.BUFFER).publish();
		List<Consumer<Event>> _subProcessors = new ArrayList<>();
		_subProcessors.add(event -> {
			EventStorageHelper.register(eventCache, registeredVersion, event);
		});
		if (subProcessors != null && !subProcessors.isEmpty()) {
			_subProcessors.addAll(subProcessors);
		}
		_subProcessors.forEach(subProcessor -> flux.subscribe(subProcessor));
		disposable = flux.connect();
	}

	@Override
	public void accept(Event event) {
		synchronized (consumer) {
			Consumer<Event> _consumer = consumer.get();
			if (_consumer != null)
				_consumer.accept(event);
		}
	}

	public void stop() {
		disposable.dispose();
	}
}
