# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [2026-03-04] — PR #1 by @prakharchoudhary

### Added
- Initial Flink streaming pipeline with three processing phases for aircraft tracking via ADS-B data.
- Data models for aircraft records, flight segments, alert events, and lost contact events.
- Phase 1 ingestion job that parses ADS-B JSON snapshots, validates records using physics-based plausibility checks, and filters invalid data.
- Phase 2 window aggregation job that computes per-aircraft summaries over tumbling time windows including altitude ranges, speed statistics, and position bounds.
- Phase 3 stateful anomaly detection job that identifies emergency squawk codes, rapid altitude changes, implausible speeds/altitudes, and lost contact events.
- Custom ADS-B file source and snapshot format for reading historical data files.
- Aircraft record parser with comprehensive field mapping from snake_case JSON to camelCase Kotlin model.
- Field validator with configurable thresholds for ground speed, altitude, and vertical rate.
- Geographic utilities for distance calculations using the Haversine formula.
- Comprehensive test suite covering parsing, validation, ingestion, aggregation, and anomaly detection.
- IntelliJ IDEA project configuration files including code style settings and Maven integration.

---