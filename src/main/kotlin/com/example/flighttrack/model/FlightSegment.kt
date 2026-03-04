package com.example.flighttrack.model

/**
 * Aggregated summary of one aircraft within a tumbling time window.
 *
 * Produced by Phase 2 (WindowAggregationJob). One segment = one (hex, window) pair.
 */
data class FlightSegment(
    val hex: String,
    val flight: String?,            // first non-null callsign seen in the window
    val windowStartTime: Double,    // earliest snapshotTime in window (Unix epoch, seconds)
    val windowEndTime: Double,      // latest snapshotTime in window
    val recordCount: Int,           // number of raw records aggregated

    val minAltBaro: Int?,
    val maxAltBaro: Int?,

    val minGroundSpeed: Double?,
    val maxGroundSpeed: Double?,
    val avgGroundSpeed: Double?,

    val minLat: Double?,
    val maxLat: Double?,
    val minLon: Double?,
    val maxLon: Double?,

    val squawks: List<String>,      // distinct squawk codes observed
    val emergencies: List<String>,  // non-"none" emergency values observed
)
