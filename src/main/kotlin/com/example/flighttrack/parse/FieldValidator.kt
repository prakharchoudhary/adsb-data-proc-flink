package com.example.flighttrack.parse

import com.example.flighttrack.model.AircraftRecord

/**
 * Applies physics-based implausibility rules to a parsed [AircraftRecord].
 *
 * Validation is intentionally lenient: the goal is to catch sensor glitches
 * and data-feed errors, not to enforce strict operational envelopes.
 * Any record that fails validation is dropped by Phase 1 before storage.
 *
 * All thresholds are configurable via the companion object constants.
 */
object FieldValidator {

    // Speed: Mach 1.2 at 35 000 ft ≈ 795 kt; 800 kt gives a small safety margin.
    const val MAX_GROUND_SPEED_KT = 800.0

    // Altitude: SR-71 service ceiling ~85 000 ft; 60 000 ft covers all civil traffic.
    const val MAX_ALT_BARO_FT    = 60_000
    // Below mean sea level — allow a modest margin for airports below sea level.
    const val MIN_ALT_BARO_FT    = -2_000

    // Vertical rate: 6 000 fpm is an extreme emergency descent; 10 000 fpm is impossible.
    const val MAX_BARO_RATE_FPM  = 10_000

    data class ValidationResult(
        val valid: Boolean,
        val reasons: List<String>,
    )

    fun validate(record: AircraftRecord): ValidationResult {
        val reasons = mutableListOf<String>()

        record.groundSpeed?.let { gs ->
            if (gs < 0)                  reasons += "Negative ground speed: $gs kt"
            if (gs > MAX_GROUND_SPEED_KT) reasons += "Implausible ground speed: $gs kt (max $MAX_GROUND_SPEED_KT)"
        }

        record.altBaro?.let { alt ->
            if (alt > MAX_ALT_BARO_FT) reasons += "Altitude above ceiling: $alt ft"
            if (alt < MIN_ALT_BARO_FT) reasons += "Altitude below floor: $alt ft"
        }

        record.baroRate?.let { rate ->
            if (rate > MAX_BARO_RATE_FPM)  reasons += "Implausible climb rate: $rate fpm"
            if (rate < -MAX_BARO_RATE_FPM) reasons += "Implausible descent rate: $rate fpm"
        }

        record.lat?.let { lat ->
            if (lat < -90.0 || lat > 90.0) reasons += "Latitude out of range: $lat"
        }

        record.lon?.let { lon ->
            if (lon < -180.0 || lon > 180.0) reasons += "Longitude out of range: $lon"
        }

        record.track?.let { trk ->
            if (trk < 0.0 || trk >= 360.0) reasons += "Track out of range: $trk°"
        }

        return ValidationResult(reasons.isEmpty(), reasons)
    }
}
