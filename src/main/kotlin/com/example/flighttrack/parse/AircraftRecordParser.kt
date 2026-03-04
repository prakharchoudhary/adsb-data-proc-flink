package com.example.flighttrack.parse

import com.example.flighttrack.model.AircraftRecord
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.InputStream

/**
 * Parses the native ADS-B exchange JSON format into [AircraftRecord] objects.
 *
 * The raw file structure is:
 * ```json
 * { "now": 1772323199.001, "messages": 3303995385, "aircraft": [ … ] }
 * ```
 *
 * Each aircraft node uses snake_case keys; this parser performs the mapping
 * to the camelCase Kotlin model. Returns `null` for any record missing the
 * mandatory `hex` field.
 *
 * NOTE: The ADSBexchange server may return plain JSON despite a `.gz` file
 * extension — callers should handle decompression before passing the stream.
 */
class AircraftRecordParser(private val mapper: ObjectMapper = ObjectMapper()) {

    /**
     * Parse a full snapshot stream and return all valid aircraft records,
     * each stamped with the snapshot's `now` timestamp.
     */
    fun parseSnapshot(inputStream: InputStream): List<AircraftRecord> {
        val root = mapper.readTree(inputStream)
        val snapshotTime = root.path("now").asDouble()
        val aircraftArray = root.path("aircraft")
        if (!aircraftArray.isArray) return emptyList()
        return aircraftArray.mapNotNull { node -> parseRecord(node, snapshotTime) }
    }

    /**
     * Parse a single aircraft JSON node. Returns `null` if the required
     * `hex` field is absent or blank.
     */
    fun parseRecord(node: JsonNode, snapshotTime: Double): AircraftRecord? {
        val hex = node.path("hex").asText().trim().ifEmpty { return null }

        return AircraftRecord(
            hex            = hex,
            snapshotTime   = snapshotTime,
            type           = node.textOrNull("type"),
            flight         = node.path("flight").asText().trim().ifEmpty { null },
            registration   = node.textOrNull("r"),
            aircraftType   = node.textOrNull("t"),
            // alt_baro can be the string "ground" for aircraft on the surface
            altBaro        = node.intOrNullSkipText("alt_baro"),
            altGeom        = node.intOrNullSkipText("alt_geom"),
            groundSpeed    = node.doubleOrNull("gs"),
            track          = node.doubleOrNull("track"),
            baroRate       = node.intOrNull("baro_rate"),
            geomRate       = node.intOrNull("geom_rate"),
            squawk         = node.textOrNull("squawk"),
            emergency      = node.textOrNull("emergency"),
            category       = node.textOrNull("category"),
            lat            = node.doubleOrNull("lat"),
            lon            = node.doubleOrNull("lon"),
            nic            = node.intOrNull("nic"),
            seen           = node.doubleOrNull("seen"),
            seenPos        = node.doubleOrNull("seen_pos"),
            rssi           = node.doubleOrNull("rssi"),
            messages       = if (node.has("messages")) node.path("messages").asLong() else null,
            navQnh         = node.doubleOrNull("nav_qnh"),
            navAltitudeMcp = node.intOrNull("nav_altitude_mcp"),
            navHeading     = node.doubleOrNull("nav_heading"),
            // mlat/tisb are arrays of field names that used multilateration; non-empty → true
            mlat           = node.path("mlat").let { it.isArray && it.size() > 0 },
            tisb           = node.path("tisb").let { it.isArray && it.size() > 0 },
        )
    }

    // ── Private helpers ─────────────────────────────────────────────────────

    private fun JsonNode.textOrNull(field: String): String? =
        path(field).takeIf { it.isTextual }?.asText()

    private fun JsonNode.intOrNull(field: String): Int? =
        path(field).takeIf { it.isNumber }?.asInt()

    /** Like [intOrNull] but also swallows textual nodes (e.g. "ground"). */
    private fun JsonNode.intOrNullSkipText(field: String): Int? =
        path(field).takeIf { it.isNumber }?.asInt()

    private fun JsonNode.doubleOrNull(field: String): Double? =
        path(field).takeIf { it.isNumber }?.asDouble()
}
