package org.lvps.model;

import java.time.Instant;

/**
 * Immutable price record.
 */
public record PriceRecord(String instrumentId, Instant asOf, String payload) {}
