package com.atomosphere.eventstorage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.atomosphere.eventstorage.exception.EventStorageException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@AllArgsConstructor
@Accessors(fluent = true)
public class SubscribedEvents implements Iterable<SubscribedEvent> {
	private final EventSubscriber eventSubscriber;

	private List<SubscribedEvent> list = new ArrayList<>();

	SubscribedEvents(EventSubscriber eventSubscriber) {
		this.eventSubscriber = eventSubscriber;
	}

	public void acknowledge() throws EventStorageException {
		for (SubscribedEvent subscribedEvent : list) {
			eventSubscriber.acknowledge(subscribedEvent);
		}
	}

	@Override
	public Iterator<SubscribedEvent> iterator() {
		return list.iterator();
	}
}
