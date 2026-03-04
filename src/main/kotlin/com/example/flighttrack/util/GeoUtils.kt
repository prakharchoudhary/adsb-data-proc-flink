package com.example.flighttrack.util

import kotlin.math.*

/**
 * Geodetic utilities used across all pipeline phases.
 */
object GeoUtils {

    /** Earth's mean radius in nautical miles (NM). */
    const val EARTH_RADIUS_NM = 3_440.065

    /** Earth's mean radius in kilometres. */
    const val EARTH_RADIUS_KM = 6_371.009

    /**
     * Haversine great-circle distance between two WGS-84 coordinates.
     *
     * @return Distance in nautical miles.
     */
    fun haversineNm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double =
        haversine(lat1, lon1, lat2, lon2, EARTH_RADIUS_NM)

    /**
     * Haversine great-circle distance between two WGS-84 coordinates.
     *
     * @return Distance in kilometres.
     */
    fun haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double =
        haversine(lat1, lon1, lat2, lon2, EARTH_RADIUS_KM)

    /**
     * Initial bearing from point 1 → point 2, in degrees [0, 360).
     */
    fun bearingDeg(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double {
        val φ1 = Math.toRadians(lat1)
        val φ2 = Math.toRadians(lat2)
        val dλ = Math.toRadians(lon2 - lon1)
        val y = sin(dλ) * cos(φ2)
        val x = cos(φ1) * sin(φ2) - sin(φ1) * cos(φ2) * cos(dλ)
        return (Math.toDegrees(atan2(y, x)) + 360.0) % 360.0
    }

    // ── Private ──────────────────────────────────────────────────────────────

    private fun haversine(
        lat1: Double, lon1: Double,
        lat2: Double, lon2: Double,
        radius: Double,
    ): Double {
        val dLat = Math.toRadians(lat2 - lat1)
        val dLon = Math.toRadians(lon2 - lon1)
        val a = sin(dLat / 2).pow(2) +
                cos(Math.toRadians(lat1)) * cos(Math.toRadians(lat2)) * sin(dLon / 2).pow(2)
        return 2 * radius * asin(sqrt(a))
    }
}
