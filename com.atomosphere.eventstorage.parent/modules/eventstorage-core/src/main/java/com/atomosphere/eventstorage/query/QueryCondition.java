package com.atomosphere.eventstorage.query;

import com.atomosphere.eventstorage.model.ColferObject;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class QueryCondition {
	private final ColferObject key;
	private final ColferObject condition;
}
