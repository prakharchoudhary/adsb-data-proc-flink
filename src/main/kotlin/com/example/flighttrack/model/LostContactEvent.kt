package com.example.flighttrack.model

/**
 * Emitted when an aircraft that was present in snapshot window N-1
 * is absent for longer than the configured lost-contact threshold.
 *
 * Produced by Phase 3 (StatefulDetectionJob).
 */
data class LostContactEvent(
    val hex: String,
    val flight: String?,
    val lastSeenTime: Double,   // snapshotTime of its final observed record
    val lastLat: Double?,
    val lastLon: Double?,
    val lastAltBaro: Int?,
)
