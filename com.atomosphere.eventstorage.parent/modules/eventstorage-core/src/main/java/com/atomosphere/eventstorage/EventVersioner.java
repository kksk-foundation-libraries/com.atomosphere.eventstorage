package com.atomosphere.eventstorage;

import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.cache.processor.EntryProcessor;

import org.apache.ignite.IgniteCache;

import com.atomosphere.eventstorage.entry.processor.EventRegisterEntryProcessor;
import com.atomosphere.eventstorage.model.Binary;
import com.atomosphere.eventstorage.model.colfer.Event;
import com.atomosphere.eventstorage.model.colfer.EventVersion;

public class EventVersioner implements Function<Event, Event> {
	private static final EntryProcessor<Binary, Integer, Integer> register = new EventRegisterEntryProcessor();
	private final IgniteCache<Binary, byte[]> eventCache;
	private final IgniteCache<Binary, Integer> registeredVersion;

	public EventVersioner( //
			IgniteCache<Binary, byte[]> eventCache, //
			IgniteCache<Binary, Integer> registeredVersion //
	) {
		this.eventCache = eventCache;
		this.registeredVersion = registeredVersion;
	}

	@Override
	public Event apply(Event event) {
		Binary binKey = Binary.of(event.getKey());
		Lock lock = registeredVersion.lock(binKey);
		try {
			Integer version = registeredVersion.invoke(binKey, register);
			EventVersion eventVersion = new EventVersion().withKey(event.getKey()).withVersion(version);
			eventCache.put(Binary.of(eventVersion), event.withVersion(version).marshal());
		} finally {
			lock.unlock();
		}
		return event;
	}

}
