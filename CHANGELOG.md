# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [2026-03-05] — PR #2 by @prakharchoudhary

### Added
- Exercise 2-A: Tumbling 5-minute window aggregation to compute average ground speed per aircraft using event-time processing.
- Exercise 2-B: Sliding 10-minute window with 2-minute slide to track peak altitude for each aircraft across overlapping time windows.
- Exercise 2-C: Session window with 30-minute gap timeout to detect distinct flight sessions, automatically merging sessions when late records bridge gaps.
- Exercise 2-D: Side output channel for late records with lateness annotation in milliseconds for watermark calibration monitoring.
- `AvgSpeedAggregateFunction` for incremental computation of average ground speed within tumbling windows.
- `PeakAltAggregateFunction` for tracking maximum altitude within sliding windows.
- `SessionAggregateFunction` with merge logic to handle session window combining when late records arrive.
- `SpeedWindowFunction`, `AltWindowFunction`, and `SessionWindowFunction` process functions to emit `FlightSegment` results with window metadata.
- `LateRecordTagger` to annotate late records with event time and lateness duration for analysis.
- `SegmentToJsonMapper` and `LateRecordToJsonMapper` for serializing window results and late records to NDJSON format.
- Four separate output sinks: avg-speed, peak-alt, sessions, and late-records directories.

### Changed
- `WindowAggregationJob` now implements complete Phase 2 streaming aggregation pipeline instead of placeholder stub.
- Flink execution mode configured as STREAMING to observe real watermark mechanics and window triggering behavior.
- Event-time watermark strategy with 15-minute bounded out-of-orderness tolerance applied to source stream.
- `bin/flink-local.sh` run command now supports `--phase` flag to select phase1, phase2, or phase3 jobs with automatic class resolution.
- `bin/flink-local.sh` run command now supports `--class` flag to override the default job class for a given phase.
- Script now validates that the specified main class exists in the compiled JAR before job submission.
- Default output directory for run command now determined by phase: `./data/phase1-out`, `./data/phase2-out`, or `./data/phase3-out`.
- Status check command now returns exit code 1 when Flink UI is unreachable instead of exiting the script.

### Fixed
- Event-time timestamp assignment now correctly converts `snapshotTime` from seconds (Double) to milliseconds (Long) for Flink's time system.

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

---