package com.atomosphere.eventstorage.exception;

public class EventStorageException extends Exception {
	/** serialVersionUID */
	private static final long serialVersionUID = -7787670809682350068L;

	public EventStorageException() {
		super();
	}

	public EventStorageException(String message, Throwable cause) {
		super(message, cause);
	}

	public EventStorageException(String message) {
		super(message);
	}

	public EventStorageException(Throwable cause) {
		super(cause);
	}
}
