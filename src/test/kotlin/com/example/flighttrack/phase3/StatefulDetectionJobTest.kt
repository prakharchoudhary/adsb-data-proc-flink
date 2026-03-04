package com.example.flighttrack.phase3

import com.example.flighttrack.model.AircraftRecord
import com.example.flighttrack.model.AlertType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Tests for Phase 3 anomaly-detection logic via [AnomalyDetector].
 *
 * Each test corresponds to one of the five fixture scenarios defined in
 * data/samples/fixtures.json (§ 7.2).
 */
class StatefulDetectionJobTest {

    // ── Scenario 1 – normal cruise: no alerts ─────────────────────────────────

    @Test
    fun `clean aircraft produces no alerts and no lost-contact events`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0,  squawk = "1234", baroRate = -64),
            record("aa1234", snapshotTime = 5.0,  squawk = "1234", baroRate = -64),
        )

        val result = AnomalyDetector.detect(records)

        assertTrue(result.alerts.isEmpty())
        assertTrue(result.lostEvents.isEmpty())
    }

    // ── Scenario 4 – emergency squawk 7700 triggers an alert ─────────────────

    @Test
    fun `squawk 7700 emits EMERGENCY_SQUAWK alert`() {
        val records = listOf(
            record("dd3456", snapshotTime = 0.0, squawk = "7700", emergency = "general"),
        )

        val result = AnomalyDetector.detect(records)

        assertEquals(1, result.alerts.size)
        assertEquals(AlertType.EMERGENCY_SQUAWK, result.alerts[0].alertType)
        assertEquals("dd3456", result.alerts[0].hex)
        assertTrue(result.alerts[0].detail.contains("7700"))
    }

    @Test
    fun `squawk 7600 and 7500 also emit EMERGENCY_SQUAWK alerts`() {
        val records = listOf(
            record("hex001", snapshotTime = 0.0, squawk = "7600"),
            record("hex002", snapshotTime = 0.0, squawk = "7500"),
        )

        val result = AnomalyDetector.detect(records)

        val types = result.alerts.map { it.alertType }
        assertEquals(2, types.count { it == AlertType.EMERGENCY_SQUAWK })
    }

    // ── Scenario 5 – lost contact after a gap ≥ threshold ────────────────────

    @Test
    fun `aircraft absent after a 60-second gap emits LostContactEvent`() {
        // Snapshot 1: "ee7890" is present
        // Snapshot 2: "ee7890" is gone (different aircraft only)
        val records = listOf(
            record("ee7890", snapshotTime = 0.0,  lat = 33.9, lon = -118.4, altBaro = 10_000),
            record("xx0001", snapshotTime = 60.0),   // ee7890 absent, gap = 60s
        )

        val result = AnomalyDetector.detect(records)

        assertEquals(1, result.lostEvents.size)
        val lost = result.lostEvents[0]
        assertEquals("ee7890", lost.hex)
        assertEquals(0.0,      lost.lastSeenTime)
        assertEquals(33.9,     lost.lastLat)
        assertEquals(-118.4,   lost.lastLon)
        assertEquals(10_000,   lost.lastAltBaro)
    }

    @Test
    fun `aircraft absent after a gap less than threshold does not emit LostContactEvent`() {
        val records = listOf(
            record("ee7890", snapshotTime = 0.0),
            record("xx0001", snapshotTime = 59.9),  // gap < 60s
        )

        val result = AnomalyDetector.detect(records)

        assertTrue(result.lostEvents.isEmpty())
    }

    @Test
    fun `aircraft that reappears after gap does not produce duplicate lost events`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0),
            record("aa1234", snapshotTime = 60.0),  // gap = 60s but it IS present
        )

        val result = AnomalyDetector.detect(records)

        assertTrue(result.lostEvents.isEmpty())
    }

    // ── Rapid altitude change (confirmed after 2 consecutive snapshots) ────────

    @Test
    fun `single rapid baro_rate snapshot does not emit alert`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0, baroRate = 4_000),
        )

        val result = AnomalyDetector.detect(records)

        val rapidAlerts = result.alerts.filter { it.alertType == AlertType.RAPID_ALTITUDE_CHANGE }
        assertTrue(rapidAlerts.isEmpty())
    }

    @Test
    fun `two consecutive rapid baro_rate snapshots emit RAPID_ALTITUDE_CHANGE alert`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0,  baroRate = -4_000),
            record("aa1234", snapshotTime = 5.0,  baroRate = -4_000),
        )

        val result = AnomalyDetector.detect(records)

        val rapidAlerts = result.alerts.filter { it.alertType == AlertType.RAPID_ALTITUDE_CHANGE }
        assertEquals(1, rapidAlerts.size)
        assertEquals("aa1234", rapidAlerts[0].hex)
    }

    @Test
    fun `rapid baro_rate counter resets when rate returns to normal`() {
        val records = listOf(
            record("aa1234", snapshotTime = 0.0,  baroRate = -4_000),  // count → 1
            record("aa1234", snapshotTime = 5.0,  baroRate = -100),     // reset → 0
            record("aa1234", snapshotTime = 10.0, baroRate = -4_000),  // count → 1 (no alert)
        )

        val result = AnomalyDetector.detect(records)

        val rapidAlerts = result.alerts.filter { it.alertType == AlertType.RAPID_ALTITUDE_CHANGE }
        assertTrue(rapidAlerts.isEmpty())
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun record(
        hex: String,
        snapshotTime: Double,
        squawk: String?    = "1234",
        emergency: String? = "none",
        baroRate: Int?     = null,
        lat: Double?       = null,
        lon: Double?       = null,
        altBaro: Int?      = 35_000,
        flight: String?    = null,
    ) = AircraftRecord(
        hex            = hex,
        snapshotTime   = snapshotTime,
        type           = "adsb_icao",
        flight         = flight,
        registration   = null,
        aircraftType   = null,
        altBaro        = altBaro,
        altGeom        = null,
        groundSpeed    = 450.0,
        track          = null,
        baroRate       = baroRate,
        geomRate       = null,
        squawk         = squawk,
        emergency      = emergency,
        category       = null,
        lat            = lat,
        lon            = lon,
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
