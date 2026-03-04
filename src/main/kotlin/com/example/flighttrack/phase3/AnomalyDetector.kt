package com.example.flighttrack.phase3

import com.example.flighttrack.model.AircraftRecord
import com.example.flighttrack.model.AlertEvent
import com.example.flighttrack.model.AlertType
import com.example.flighttrack.model.LostContactEvent

/**
 * Stateful anomaly-detection logic for Phase 3.
 *
 * Processes a time-ordered stream of [AircraftRecord]s and emits:
 *  - [AlertEvent]s for emergency squawks and sustained rapid altitude changes.
 *  - [LostContactEvent]s when an aircraft disappears for ≥ [lostThresholdSecs].
 *
 * This object is stateless itself — all per-aircraft state lives inside [detect]
 * as local mutable maps, making it straightforward to unit-test.
 * [StatefulDetectionJob] wires this into the streaming runtime.
 */
object AnomalyDetector {

    val EMERGENCY_SQUAWKS    = setOf("7700", "7600", "7500")
    const val RAPID_RATE_FPM = 3_000           // |baro_rate| above this triggers tracking
    const val RAPID_CONSECUTIVE = 2            // how many consecutive snapshots to confirm alert
    const val LOST_THRESHOLD_SECS = 60.0       // gap before declaring lost contact

    data class DetectionResult(
        val alerts: List<AlertEvent>,
        val lostEvents: List<LostContactEvent>,
    )

    /**
     * Run detection over a flat list of records.
     *
     * Records need not be pre-sorted; grouping by snapshot time is done internally.
     */
    fun detect(records: List<AircraftRecord>): DetectionResult {
        val alerts     = mutableListOf<AlertEvent>()
        val lostEvents = mutableListOf<LostContactEvent>()

        // Per-aircraft mutable state
        val prevHexes      = mutableMapOf<String, AircraftRecord>() // hex → last record seen
        val rapidRateCount = mutableMapOf<String, Int>()            // hex → consecutive high-rate count

        val bySnapshot = records
            .groupBy { it.snapshotTime }
            .toSortedMap()

        val times = bySnapshot.keys.toList()

        for ((idx, snapTime) in times.withIndex()) {
            val snapshot    = bySnapshot.getValue(snapTime)
            val currentHexes = snapshot.associateBy { it.hex }

            // ── Lost-contact detection ────────────────────────────────────────
            if (idx > 0) {
                val elapsed = snapTime - times[idx - 1]
                if (elapsed >= LOST_THRESHOLD_SECS) {
                    for ((hex, prev) in prevHexes) {
                        if (hex !in currentHexes) {
                            lostEvents += LostContactEvent(
                                hex          = hex,
                                flight       = prev.flight,
                                lastSeenTime = prev.snapshotTime,
                                lastLat      = prev.lat,
                                lastLon      = prev.lon,
                                lastAltBaro  = prev.altBaro,
                            )
                        }
                    }
                }
            }

            // ── Per-record alert detection ────────────────────────────────────
            for (record in snapshot) {

                // Emergency squawk
                if (record.squawk in EMERGENCY_SQUAWKS) {
                    alerts += AlertEvent(
                        hex          = record.hex,
                        flight       = record.flight,
                        snapshotTime = record.snapshotTime,
                        alertType    = AlertType.EMERGENCY_SQUAWK,
                        detail       = "Squawk ${record.squawk}",
                    )
                }

                // Rapid altitude change (confirmed after RAPID_CONSECUTIVE snapshots)
                val rate = record.baroRate ?: 0
                if (kotlin.math.abs(rate) > RAPID_RATE_FPM) {
                    val count = (rapidRateCount[record.hex] ?: 0) + 1
                    rapidRateCount[record.hex] = count
                    if (count >= RAPID_CONSECUTIVE) {
                        alerts += AlertEvent(
                            hex          = record.hex,
                            flight       = record.flight,
                            snapshotTime = record.snapshotTime,
                            alertType    = AlertType.RAPID_ALTITUDE_CHANGE,
                            detail       = "Baro rate $rate fpm for $count consecutive snapshots",
                        )
                    }
                } else {
                    rapidRateCount[record.hex] = 0
                }
            }

            // Advance state: only keep aircraft present in this snapshot
            prevHexes.clear()
            prevHexes.putAll(currentHexes)
        }

        return DetectionResult(alerts, lostEvents)
    }
}
