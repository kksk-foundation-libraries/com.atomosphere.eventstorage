package com.atomosphere.eventstorage;

import com.atomosphere.eventstorage.exception.EventStorageException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@AllArgsConstructor
@Accessors(fluent = true)
public class SubscribedEvent {
	private final EventSubscriber eventSubscriber;

	private Event event;
	private int version;

	SubscribedEvent(EventSubscriber eventSubscriber) {
		this.eventSubscriber = eventSubscriber;
	}

	public void acknowledge() throws EventStorageException {
		this.eventSubscriber.acknowledge(this);
	}
}
