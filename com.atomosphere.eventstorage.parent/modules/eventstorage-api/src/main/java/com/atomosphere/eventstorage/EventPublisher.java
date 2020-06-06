package com.atomosphere.eventstorage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.atomosphere.eventstorage.exception.EventStorageException;

public interface EventPublisher {
	static EventStorageException unwrap(Exception e) {
		if (e instanceof ExecutionException) {
			Throwable cause = e.getCause();
			if (cause != null) {
				if (cause instanceof EventStorageException) {
					return (EventStorageException) cause;
				} else {
					return new EventStorageException(cause);
				}
			}
		}
		return new EventStorageException(e);
	}

	Future<Integer> async(Event event) throws EventStorageException;

	default int sync(Event event) throws EventStorageException {
		try {
			return async(event).get();
		} catch (Exception e) {
			throw unwrap(e);
		}
	}
}
