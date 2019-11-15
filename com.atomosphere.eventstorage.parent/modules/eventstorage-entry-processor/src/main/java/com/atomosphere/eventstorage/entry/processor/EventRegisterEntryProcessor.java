package com.atomosphere.eventstorage.entry.processor;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import com.atomosphere.eventstorage.model.Binary;

public class EventRegisterEntryProcessor implements EntryProcessor<Binary, Integer, Integer> {

	@Override
	public Integer process(MutableEntry<Binary, Integer> entry, Object... arguments) throws EntryProcessorException {
		Integer val = 1;
		if (entry.exists()) {
			val = entry.getValue() + 1;
		}
		entry.setValue(val);
		return val;
	}

}
