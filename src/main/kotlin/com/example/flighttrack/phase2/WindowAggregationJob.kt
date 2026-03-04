package com.example.flighttrack.phase2

/**
 * Phase 2 – Window Aggregation
 *
 * Entry point for the aggregation pipeline.
 * Consumes [com.example.flighttrack.model.AircraftRecord] records from Phase 1,
 * groups them by aircraft hex within tumbling time windows, and emits one
 * [com.example.flighttrack.model.FlightSegment] per (hex, window) pair.
 */
class WindowAggregationJob
