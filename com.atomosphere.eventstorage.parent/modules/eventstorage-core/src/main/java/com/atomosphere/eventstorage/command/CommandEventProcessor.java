package com.atomosphere.eventstorage.command;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import javax.cache.processor.EntryProcessor;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.entry.processor.EventRegisterEntryProcessor;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class CommandEventProcessor implements Consumer<Event> {
	private final AtomicReference<Consumer<Event>> consumer = new AtomicReference<>(null);
	private final Disposable disposable;

	private static final EntryProcessor<Binary, Integer, Integer> register = new EventRegisterEntryProcessor();

	public CommandEventProcessor(IgniteCache<Binary, byte[]> eventCache, IgniteCache<Binary, Integer> registeredVersion) {
		Flux<Event> flux = Flux.<Event>create(sink -> {
			consumer.set(event -> {
				sink.next(event);
			});
			sink.onCancel(() -> {
				consumer.set(null);
			});
		}, FluxSink.OverflowStrategy.BUFFER);
		disposable = flux.subscribe(event -> {
			Binary binKey = Binary.of(event.getKey());
			Lock lock = registeredVersion.lock(binKey);
			try {
				Integer version = registeredVersion.invoke(binKey, register);
				EventVersion eventVersion = new EventVersion().withKey(event.getKey()).withVersion(version);
				eventCache.put(Binary.of(eventVersion), event.withVersion(version).marshal());
			} finally {
				lock.unlock();
			}
		});
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
