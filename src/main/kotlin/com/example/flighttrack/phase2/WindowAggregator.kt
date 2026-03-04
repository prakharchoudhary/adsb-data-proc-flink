package com.example.flighttrack.phase2

import com.example.flighttrack.model.AircraftRecord
import com.example.flighttrack.model.FlightSegment

/**
 * Pure aggregation logic for Phase 2.
 *
 * Groups a flat list of [AircraftRecord]s by aircraft hex within tumbling
 * time windows of [windowSecs] seconds and returns one [FlightSegment]
 * per (hex, window-bucket) pair, sorted by hex then window start time.
 *
 * This object is stateless and has no I/O concerns — [WindowAggregationJob]
 * wires it into the streaming/batch runtime.
 */
object WindowAggregator {

    /**
     * @param records    Flat list of records (any order is fine; time bucketing is applied internally).
     * @param windowSecs Tumbling window length in seconds. Default 300 s (5 minutes).
     */
    fun aggregate(records: List<AircraftRecord>, windowSecs: Double = 300.0): List<FlightSegment> =
        records
            .groupBy { r -> r.hex to windowBucket(r.snapshotTime, windowSecs) }
            .map { (key, recs) -> buildSegment(key.first, recs) }
            .sortedWith(compareBy({ it.hex }, { it.windowStartTime }))

    // ── Internals ─────────────────────────────────────────────────────────────

    private fun windowBucket(time: Double, windowSecs: Double): Long =
        (time / windowSecs).toLong()

    private fun buildSegment(hex: String, records: List<AircraftRecord>): FlightSegment {
        val times  = records.map { it.snapshotTime }
        val alts   = records.mapNotNull { it.altBaro }
        val speeds = records.mapNotNull { it.groundSpeed }
        val lats   = records.mapNotNull { it.lat }
        val lons   = records.mapNotNull { it.lon }

        return FlightSegment(
            hex             = hex,
            flight          = records.firstNotNullOfOrNull { it.flight },
            windowStartTime = times.min(),
            windowEndTime   = times.max(),
            recordCount     = records.size,
            minAltBaro      = alts.minOrNull(),
            maxAltBaro      = alts.maxOrNull(),
            minGroundSpeed  = speeds.minOrNull(),
            maxGroundSpeed  = speeds.maxOrNull(),
            avgGroundSpeed  = speeds.takeIf { it.isNotEmpty() }?.average(),
            minLat          = lats.minOrNull(),
            maxLat          = lats.maxOrNull(),
            minLon          = lons.minOrNull(),
            maxLon          = lons.maxOrNull(),
            squawks         = records.mapNotNull { it.squawk }.distinct(),
            emergencies     = records.mapNotNull { it.emergency }
                                     .filter { it != "none" }
                                     .distinct(),
        )
    }
}
