package com.example.flighttrack.phase2

import com.example.flighttrack.model.AircraftRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Tests for Phase 2 window-aggregation logic.
 *
 * All tests call the pure [aggregate] helper directly so no file I/O is needed.
 * Window size is set to 300 s (5 minutes) throughout unless stated otherwise.
 */
class WindowAggregationJobTest {

    // ── Grouping behaviour ────────────────────────────────────────────────────

    @Test
    fun `records for the same hex in the same window produce one segment`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0,   altBaro = 34000, gs = 440.0),
            record("aa1234", snapshotTime = 5.0,   altBaro = 34200, gs = 445.0),
            record("aa1234", snapshotTime = 10.0,  altBaro = 34500, gs = 450.0),
        )

        val segments = aggregate(records)

        assertEquals(1, segments.size)
        assertEquals("aa1234", segments[0].hex)
        assertEquals(3, segments[0].recordCount)
    }

    @Test
    fun `records for the same hex in different windows produce two segments`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0),    // window bucket 0
            record("aa1234", snapshotTime = 300.0),  // window bucket 1
        )

        val segments = aggregate(records, windowSecs = 300.0)

        assertEquals(2, segments.size)
        assertEquals(0.0,   segments[0].windowStartTime)
        assertEquals(300.0, segments[1].windowStartTime)
    }

    @Test
    fun `records for two different hexes in the same window produce two segments`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0),
            record("bb5678", snapshotTime = 5.0),
        )

        val segments = aggregate(records)

        assertEquals(2, segments.size)
        val hexes = segments.map { it.hex }.toSet()
        assertEquals(setOf("aa1234", "bb5678"), hexes)
    }

    // ── Aggregate statistics ──────────────────────────────────────────────────

    @Test
    fun `altitude min and max are computed correctly`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0,  altBaro = 33000),
            record("aa1234", snapshotTime = 5.0,  altBaro = 35000),
            record("aa1234", snapshotTime = 10.0, altBaro = 34000),
        )

        val seg = aggregate(records)[0]

        assertEquals(33000, seg.minAltBaro)
        assertEquals(35000, seg.maxAltBaro)
    }

    @Test
    fun `ground speed average is computed correctly`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0,  gs = 400.0),
            record("aa1234", snapshotTime = 5.0,  gs = 500.0),
        )

        val seg = aggregate(records)[0]

        assertEquals(400.0, seg.minGroundSpeed)
        assertEquals(500.0, seg.maxGroundSpeed)
        assertEquals(450.0, seg.avgGroundSpeed!!, 0.001)
    }

    @Test
    fun `null altitude records do not affect stats`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0, altBaro = null),
            record("aa1234", snapshotTime = 5.0, altBaro = 35000),
        )

        val seg = aggregate(records)[0]

        assertEquals(35000, seg.minAltBaro)
        assertEquals(35000, seg.maxAltBaro)
    }

    // ── Emergency and squawk roll-up ──────────────────────────────────────────

    @Test
    fun `emergency squawk is collected into segment emergencies list`() {
        val records = listOf(
            record("dd3456", snapshotTime = 0.0, squawk = "7700", emergency = "general"),
        )

        val seg = aggregate(records)[0]

        assertFalse(seg.emergencies.isEmpty())
        assertEquals("general", seg.emergencies[0])
        assertEquals("7700", seg.squawks[0])
    }

    @Test
    fun `none emergency is excluded from segment emergencies list`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0, emergency = "none"),
        )

        val seg = aggregate(records)[0]

        assertTrue(seg.emergencies.isEmpty())
    }

    // ── Output ordering ───────────────────────────────────────────────────────

    @Test
    fun `segments are sorted by hex then by window start time`() {
        val records = listOf(
            record("zz9999", snapshotTime = 0.0),
            record("aa1111", snapshotTime = 0.0),
            record("aa1111", snapshotTime = 300.0),
        )

        val segments = aggregate(records, windowSecs = 300.0)

        assertEquals("aa1111", segments[0].hex)
        assertEquals(0.0,      segments[0].windowStartTime)
        assertEquals("aa1111", segments[1].hex)
        assertEquals(300.0,    segments[1].windowStartTime)
        assertEquals("zz9999", segments[2].hex)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun aggregate(
        records: List<AircraftRecord>,
        windowSecs: Double = 300.0,
    ) = com.example.flighttrack.phase2.WindowAggregator.aggregate(records, windowSecs)

    private fun record(
        hex: String,
        snapshotTime: Double,
        altBaro: Int?    = 35000,
        gs: Double?      = 450.0,
        squawk: String?  = "1234",
        emergency: String? = "none",
        flight: String?  = null,
    ) = AircraftRecord(
        hex            = hex,
        snapshotTime   = snapshotTime,
        type           = "adsb_icao",
        flight         = flight,
        registration   = null,
        aircraftType   = null,
        altBaro        = altBaro,
        altGeom        = null,
        groundSpeed    = gs,
        track          = null,
        baroRate       = null,
        geomRate       = null,
        squawk         = squawk,
        emergency      = emergency,
        category       = null,
        lat            = null,
        lon            = null,
        nic            = null,
        seen           = 1.0,
        seenPos        = null,
        rssi           = -20.0,
        messages       = 100L,
        navQnh         = null,
        navAltitudeMcp = null,
        navHeading     = null,
        mlat           = false,
        tisb           = false,
    )
}
