package com.example.flighttrack.phase3

/**
 * Phase 3 – Stateful Detection
 *
 * Entry point for the anomaly-detection pipeline.
 * Consumes [com.example.flighttrack.model.FlightSegment] records from Phase 2
 * and maintains per-aircraft state to detect:
 *   - Emergency squawks (7700 / 7600 / 7500)  → [com.example.flighttrack.model.AlertEvent]
 *   - Rapid sustained altitude change           → [com.example.flighttrack.model.AlertEvent]
 *   - Lost contact (absent beyond threshold)   → [com.example.flighttrack.model.LostContactEvent]
 */
class StatefulDetectionJob
