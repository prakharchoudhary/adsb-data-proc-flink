package com.example.flighttrack.parse

import com.example.flighttrack.model.AircraftRecord
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class AircraftRecordParserTest {

    private val parser = AircraftRecordParser()
    private val mapper = ObjectMapper()

    // ── parseSnapshot ─────────────────────────────────────────────────────────

    @Test
    fun `parseSnapshot with valid JSON returns one record per aircraft`() {
        val json = """
            {
              "now": 1772323200.0,
              "messages": 1000000,
              "aircraft": [
                { "hex": "aa1234", "type": "adsb_icao", "mlat": [], "tisb": [],
                  "messages": 500, "seen": 0.5, "rssi": -20.0 },
                { "hex": "bb5678", "type": "adsb_icao", "mlat": [], "tisb": [],
                  "messages": 300, "seen": 1.0, "rssi": -25.0 }
              ]
            }
        """.trimIndent()

        val records = parser.parseSnapshot(json.byteInputStream())

        assertEquals(2, records.size)
        assertEquals("aa1234", records[0].hex)
        assertEquals("bb5678", records[1].hex)
        assertEquals(1772323200.0, records[0].snapshotTime)
    }

    @Test
    fun `parseSnapshot with empty aircraft array returns empty list`() {
        val json = """{"now": 1772323200.0, "messages": 0, "aircraft": []}"""

        val records = parser.parseSnapshot(json.byteInputStream())

        assertTrue(records.isEmpty())
    }

    // ── parseRecord – field mapping ───────────────────────────────────────────

    @Test
    fun `parseRecord maps all core fields correctly`() {
        // Scenario 1 – normal cruise record (mirrors fixtures scenario-01)
        val node = mapper.readTree(
            """
            {
              "hex":           "aa1234",
              "type":          "adsb_icao",
              "flight":        "UAL123  ",
              "r":             "N12345",
              "t":             "B738",
              "alt_baro":      35000,
              "alt_geom":      34850,
              "gs":            450.0,
              "track":         270.5,
              "baro_rate":     -64,
              "squawk":        "1234",
              "emergency":     "none",
              "category":      "A3",
              "lat":           40.7128,
              "lon":           -74.0060,
              "nic":           8,
              "seen":          0.5,
              "seen_pos":      0.9,
              "rssi":          -22.0,
              "messages":      12345,
              "nav_qnh":       1013.2,
              "nav_altitude_mcp": 35000,
              "nav_heading":   270.0,
              "mlat":          [],
              "tisb":          []
            }
            """.trimIndent()
        )

        val record = parser.parseRecord(node, 1772323200.0)!!

        assertEquals("aa1234",    record.hex)
        assertEquals(1772323200.0, record.snapshotTime)
        assertEquals("adsb_icao", record.type)
        assertEquals("UAL123",    record.flight)        // trailing spaces trimmed
        assertEquals("N12345",    record.registration)
        assertEquals("B738",      record.aircraftType)
        assertEquals(35000,       record.altBaro)
        assertEquals(34850,       record.altGeom)
        assertEquals(450.0,       record.groundSpeed)
        assertEquals(270.5,       record.track)
        assertEquals(-64,         record.baroRate)
        assertEquals("1234",      record.squawk)
        assertEquals("none",      record.emergency)
        assertEquals("A3",        record.category)
        assertEquals(40.7128,     record.lat)
        assertEquals(-74.0060,    record.lon)
        assertEquals(8,           record.nic)
        assertEquals(0.5,         record.seen)
        assertEquals(0.9,         record.seenPos)
        assertEquals(-22.0,       record.rssi)
        assertEquals(12345L,      record.messages)
        assertEquals(1013.2,      record.navQnh)
        assertEquals(35000,       record.navAltitudeMcp)
        assertEquals(270.0,       record.navHeading)
        assertFalse(record.mlat)
        assertFalse(record.tisb)
    }

    @Test
    fun `parseRecord with missing hex returns null`() {
        val node = mapper.readTree("""{"type": "adsb_icao", "mlat": [], "tisb": []}""")

        val result = parser.parseRecord(node, 1772323200.0)

        assertNull(result)
    }

    @Test
    fun `parseRecord with position-less record returns null lat and lon`() {
        // Scenario 2 – valid record but no position data
        val node = mapper.readTree(
            """
            {
              "hex": "bb5678", "type": "adsb_icao",
              "flight": "DAL456", "alt_baro": 30000, "gs": 420.0,
              "squawk": "2345", "mlat": [], "tisb": [], "messages": 200, "seen": 1.0, "rssi": -25.0
            }
            """.trimIndent()
        )

        val record = parser.parseRecord(node, 1772323200.0)!!

        assertNull(record.lat)
        assertNull(record.lon)
        assertEquals(30000, record.altBaro)
    }

    @Test
    fun `parseRecord with alt_baro as string 'ground' returns null altBaro`() {
        val node = mapper.readTree(
            """{"hex": "cc0001", "alt_baro": "ground", "mlat": [], "tisb": [], "messages": 10, "seen": 0.1, "rssi": -30.0}"""
        )

        val record = parser.parseRecord(node, 1772323200.0)!!

        assertNull(record.altBaro)
    }

    @Test
    fun `parseRecord with non-empty mlat array sets mlat to true`() {
        val node = mapper.readTree(
            """{"hex": "dd0001", "mlat": ["lat","lon"], "tisb": [], "messages": 5, "seen": 0.2, "rssi": -28.0}"""
        )

        val record = parser.parseRecord(node, 1772323200.0)!!

        assertTrue(record.mlat)
        assertFalse(record.tisb)
    }

    @Test
    fun `parseRecord trims blank flight to null`() {
        val node = mapper.readTree(
            """{"hex": "ee0001", "flight": "   ", "mlat": [], "tisb": [], "messages": 1, "seen": 0.1, "rssi": -30.0}"""
        )

        val record = parser.parseRecord(node, 1772323200.0)!!

        assertNull(record.flight)
    }
}
