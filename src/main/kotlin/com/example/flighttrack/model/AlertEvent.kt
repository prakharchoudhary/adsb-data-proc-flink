package com.example.flighttrack.model

enum class AlertType {
    EMERGENCY_SQUAWK,       // squawk 7700 / 7600 / 7500
    RAPID_ALTITUDE_CHANGE,  // |baro_rate| > threshold for 2+ consecutive snapshots
    IMPLAUSIBLE_SPEED,      // ground speed exceeds physical limit
    IMPLAUSIBLE_ALTITUDE,   // altitude outside plausible civil-aviation range
}

/**
 * A detected anomaly for a single aircraft at a point in time.
 *
 * Produced by Phase 3 (StatefulDetectionJob).
 */
data class AlertEvent(
    val hex: String,
    val flight: String?,
    val snapshotTime: Double,
    val alertType: AlertType,
    val detail: String,
)
