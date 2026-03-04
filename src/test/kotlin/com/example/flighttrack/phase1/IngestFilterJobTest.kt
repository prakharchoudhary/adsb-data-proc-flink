package com.example.flighttrack.phase1

import com.example.flighttrack.parse.AircraftRecordParser
import com.example.flighttrack.parse.FieldValidator
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Tests for the Phase 1 parsing and filtering logic.
 *
 * Since [IngestFilterJob] is a job-wiring stub, these tests exercise the
 * underlying [AircraftRecordParser] + [FieldValidator] pipeline directly —
 * the same components Phase 1 will orchestrate.
 */
class IngestFilterJobTest {

    private val parser    = AircraftRecordParser()
    private val validator = FieldValidator

    // ── Scenario 1 – normal cruise record passes through unchanged ────────────

    @Test
    fun `valid cruise record parses and passes validation`() {
        val snapshot = snapshotJson(
            """
            { "hex":"aa1234", "flight":"UAL123", "alt_baro":35000, "gs":450.0,
              "lat":40.71, "lon":-74.00, "squawk":"1234", "emergency":"none",
              "mlat":[], "tisb":[], "messages":100, "seen":0.5, "rssi":-22.0 }
            """
        )

        val records = parser.parseSnapshot(snapshot.byteInputStream())
        assertEquals(1, records.size)

        val result = validator.validate(records[0])
        assertTrue(result.valid, "Expected valid but got: ${result.reasons}")
    }

    // ── Scenario 2 – position-less record passes validation ───────────────────

    @Test
    fun `record without position data passes validation`() {
        val snapshot = snapshotJson(
            """
            { "hex":"bb5678", "flight":"DAL456", "alt_baro":30000, "gs":420.0,
              "mlat":[], "tisb":[], "messages":80, "seen":1.0, "rssi":-25.0 }
            """
        )

        val records = parser.parseSnapshot(snapshot.byteInputStream())
        val result  = validator.validate(records[0])

        assertNull(records[0].lat)
        assertNull(records[0].lon)
        assertTrue(result.valid)
    }

    // ── Scenario 3 – implausible speed is rejected ────────────────────────────

    @Test
    fun `record with ground speed above threshold fails validation`() {
        val snapshot = snapshotJson(
            """
            { "hex":"cc9012", "flight":"BAW789", "alt_baro":40000, "gs":1500.0,
              "lat":51.5, "lon":-0.1,
              "mlat":[], "tisb":[], "messages":50, "seen":0.3, "rssi":-20.0 }
            """
        )

        val records = parser.parseSnapshot(snapshot.byteInputStream())
        val result  = validator.validate(records[0])

        assertFalse(result.valid)
        assertTrue(result.reasons.any { "ground speed" in it.lowercase() })
    }

    @Test
    fun `record with negative ground speed fails validation`() {
        val snapshot = snapshotJson(
            """{ "hex":"ff0001", "gs":-10.0, "mlat":[], "tisb":[], "messages":1, "seen":0.1, "rssi":-30.0 }"""
        )

        val result = validator.validate(parser.parseSnapshot(snapshot.byteInputStream())[0])

        assertFalse(result.valid)
    }

    @Test
    fun `record with altitude above ceiling fails validation`() {
        val snapshot = snapshotJson(
            """{ "hex":"ff0002", "alt_baro":70000, "mlat":[], "tisb":[], "messages":1, "seen":0.1, "rssi":-30.0 }"""
        )

        val result = validator.validate(parser.parseSnapshot(snapshot.byteInputStream())[0])

        assertFalse(result.valid)
    }

    @Test
    fun `record with out-of-range latitude fails validation`() {
        val snapshot = snapshotJson(
            """{ "hex":"ff0003", "lat":95.0, "lon":0.0, "mlat":[], "tisb":[], "messages":1, "seen":0.1, "rssi":-30.0 }"""
        )

        val result = validator.validate(parser.parseSnapshot(snapshot.byteInputStream())[0])

        assertFalse(result.valid)
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private fun snapshotJson(aircraftJson: String) =
        """{"now": 1772323200.0, "messages": 999, "aircraft": [$aircraftJson]}"""
}
