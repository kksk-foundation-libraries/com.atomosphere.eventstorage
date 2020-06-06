package com.atomosphere.eventstorage;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class VersionRange {
	private final int minimum;
	private final int maximum;
}
