package com.atomosphere.eventstorage;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.atomosphere.eventstorage.exception.EventStorageException;

public interface EventStorage extends AutoCloseable {
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

	Future<Event> get(int eventType, byte[] key, int version) throws EventStorageException;

	default Event getSync(int eventType, byte[] key, int version) throws EventStorageException {
		try {
			return get(eventType, key, version).get();
		} catch (Exception e) {
			throw unwrap(e);
		}
	}

	Future<List<Event>> getAll(int eventType, byte[] key, VersionRange versionRange) throws EventStorageException;

	default List<Event> getAllSync(int eventType, byte[] key, VersionRange versionRange) throws EventStorageException {
		try {
			return getAll(eventType, key, versionRange).get();
		} catch (Exception e) {
			throw unwrap(e);
		}
	}

	Future<VersionRange> versionRange(int eventType, byte[] key) throws EventStorageException;

	default VersionRange versionRangeSync(int eventType, byte[] key) throws EventStorageException {
		try {
			return versionRange(eventType, key).get();
		} catch (Exception e) {
			throw unwrap(e);
		}
	}
}
