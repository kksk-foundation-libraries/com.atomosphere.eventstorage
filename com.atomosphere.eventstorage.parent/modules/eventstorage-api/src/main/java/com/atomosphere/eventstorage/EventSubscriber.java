package com.atomosphere.eventstorage;

import com.atomosphere.eventstorage.exception.EventStorageException;

import reactor.core.publisher.Flux;

public interface EventSubscriber {
	Flux<SubscribedEvents> stream(int eventType) throws EventStorageException;

	void acknowledge(SubscribedEvent receivedEvent) throws EventStorageException;
}
