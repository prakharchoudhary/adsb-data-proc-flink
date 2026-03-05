# Flight Tracking with Apache Flink

A multi-phase Apache Flink pipeline for real-time aircraft tracking, aggregation, and anomaly detection using ADS-B exchange data.

## Overview

This project implements a streaming data pipeline that processes ADS-B (Automatic Dependent Surveillance–Broadcast) aircraft position reports. The system ingests raw JSON snapshots, validates and filters data, aggregates flight segments over time windows, and detects anomalies such as emergency squawks, implausible speeds, rapid altitude changes, and lost contact events.

**Technology Stack:**
- **Language:** Kotlin
- **Stream Processing:** Apache Flink
- **Build Tool:** Maven
- **Java Version:** 11 (Corretto)

## Architecture

The pipeline consists of three sequential phases:

### Phase 1: Ingestion & Filtering
- **Job:** `IngestFilterJob`
- **Input:** Raw ADS-B snapshot files (JSON format)
- **Processing:**
  - Parses JSON using `AircraftRecordParser`
  - Validates records against physics-based constraints (speed, altitude, vertical rate)
  - Filters out implausible or incomplete data
- **Output:** Clean `AircraftRecord` stream

### Phase 2: Window Aggregation
- **Job:** `WindowAggregationJob`
- **Input:** Validated aircraft records from Phase 1
- **Processing:**
  - **Exercise 2-A:** Computes average ground speed per aircraft using 5-minute tumbling windows
  - **Exercise 2-B:** Tracks peak altitude per aircraft using 10-minute sliding windows (2-minute slide)
  - **Exercise 2-C:** Detects flight sessions with 30-minute session windows (gaps > 30 min close a session)
  - **Exercise 2-D:** Captures late-arriving records via side output for watermark calibration analysis
  - Assigns event-time watermarks with 15-minute bounded out-of-orderness tolerance
  - Aggregates within windows using specialized `AggregateFunction` implementations
  - Computes min/max/avg statistics for altitude, speed, position
  - Collects distinct squawk codes and emergency statuses
- **Output:** Four separate streams:
  - `FlightSegment` summaries for average speed (tumbling windows)
  - `FlightSegment` summaries for peak altitude (sliding windows)
  - `FlightSegment` summaries for flight sessions (session windows)
  - `LateRecord` annotations for records that arrived after their window closed

### Phase 3: Anomaly Detection
- **Job:** `StatefulDetectionJob`
- **Input:** Flight segments from Phase 2
- **Processing:**
  - Maintains per-aircraft state across windows
  - Detects:
    - **Emergency squawks** (7700, 7600, 7500)
    - **Rapid altitude changes** (sustained high vertical rate)
    - **Implausible speeds** (exceeds MAX_GROUND_SPEED_KT)
    - **Implausible altitudes** (outside civil aviation range)
    - **Lost contact** (aircraft missing for threshold duration)
- **Output:** `AlertEvent` and `LostContactEvent` streams

## Data Models

### AircraftRecord
Represents a single aircraft observation from one ADS-B snapshot. Includes:
- **Identity:** ICAO hex, callsign, registration, aircraft type
- **Position:** latitude, longitude, NIC quality indicator
- **Altitude:** barometric and geometric altitude, vertical rates
- **Motion:** ground speed, track/heading
- **Transponder:** squawk code, emergency status, emitter category
- **Quality:** RSSI, message count, staleness (seen/seenPos), MLAT/TIS-B flags

**POJO Compliance:** All fields are `var` with default values to satisfy Flink's POJO serialization contract (avoids Kryo overhead).

### FlightSegment
Windowed aggregate statistics for one aircraft:
- Window time range and record count
- Min/max/avg altitude, speed, position bounds
- Collected squawk codes and emergency statuses

### AlertEvent
Anomaly detection output:
- Aircraft identifier (hex, callsign)
- Timestamp of detection
- Alert type (enum)
- Descriptive detail string

### LostContactEvent
Emitted when an aircraft disappears from tracking:
- Last known position and altitude
- Timestamp of final observation

## Validation Rules

`FieldValidator` enforces the following constraints (configurable via constants):

| Field | Constraint | Default Threshold |
|---|---|---|
| Ground speed | ≤ MAX_GROUND_SPEED_KT | 800 kt |
| Barometric altitude | MIN_ALT_BARO_FT to MAX_ALT_BARO_FT | -2,000 to 60,000 ft |
| Vertical rate (baro) | \|rate\| ≤ MAX_BARO_RATE_FPM | 10,000 fpm |
| Latitude | -90° to +90° | — |
| Longitude | -180° to +180° | — |

Records violating any rule are dropped with logged reasons.

## Utilities

### GeoUtils
Provides geospatial calculations:
- **Haversine distance:** computes great-circle distance between two lat/lon coordinates (km)
- **Bounding box validation:** checks if a point lies within a rectangular region

Useful for future features like geofencing or track reconstruction.

## Project Structure

```
src/main/kotlin/com/example/flighttrack/
├── model/
│   ├── AircraftRecord.kt          # Core data model (POJO)
│   ├── FlightSegment.kt           # Windowed aggregate
│   ├── AlertEvent.kt              # Anomaly output
│   └── LostContactEvent.kt        # Lost contact output
├── parse/
│   ├── AircraftRecordParser.kt    # JSON → AircraftRecord
│   └── FieldValidator.kt          # Physics-based validation
├── phase1/
│   ├── IngestFilterJob.kt         # Phase 1 Flink job
│   ├── AdsbFileSource.kt          # File source connector
│   └── AdsbSnapshotFormat.kt      # Deserialization format
├── phase2/
│   ├── WindowAggregationJob.kt    # Phase 2 Flink job (all 4 exercises)
│   └── WindowAggregator.kt        # Aggregation logic
├── phase3/
│   ├── StatefulDetectionJob.kt    # Phase 3 Flink job
│   └── AnomalyDetector.kt         # Stateful anomaly rules
└── util/
    └── GeoUtils.kt                # Geospatial helpers

src/test/kotlin/com/example/flighttrack/
├── parse/
│   └── AircraftRecordParserTest.kt
├── phase1/
│   └── IngestFilterJobTest.kt
├── phase2/
│   └── WindowAggregationJobTest.kt
└── phase3/
    └── StatefulDetectionJobTest.kt

bin/
└── flink-local.sh                 # Local Flink cluster automation script
```

## Building

```bash
mvn clean package
```

Produces a fat JAR suitable for Flink cluster deployment.

## Running

### Using the Automation Script (Recommended)

The `bin/flink-local.sh` script provides convenient commands for managing a local Flink cluster and submitting jobs:

```bash
# Install and start local Flink cluster
./bin/flink-local.sh install
./bin/flink-local.sh start

# Check cluster status
./bin/flink-local.sh status

# Run Phase 1
./bin/flink-local.sh run --phase phase1

# Run Phase 2 with custom parallelism
./bin/flink-local.sh run --phase phase2 -p 4 ./data/raw ./data/phase2-out/run-p4

# Run Phase 3 with custom output directory
./bin/flink-local.sh run --phase phase3 ./data/raw ./data/phase3-out/run

# Stop cluster
./bin/flink-local.sh stop
```

**Script Features:**
- Automatic Flink installation and cluster lifecycle management
- Phase-aware job submission (automatically selects correct main class)
- Custom parallelism configuration via `-p` flag
- Custom input/output directory paths
- JAR validation (verifies entry class exists before submission)
- Environment variable overrides: `FLINK_VERSION`, `JOB_PARALLELISM`, `JOB_PHASE`, `UI_URL`

**Default Paths:**
- Phase 1 output: `./data/phase1-out/YYYY-MM-DD--HH`
- Phase 2 output: `./data/phase2-out/YYYY-MM-DD--HH`
- Phase 3 output: `./data/phase3-out/YYYY-MM-DD--HH`

### Manual Submission

Each phase can also be submitted directly to a Flink cluster:

```bash
# Phase 1
flink run -c com.example.flighttrack.phase1.IngestFilterJob target/flight-track-1.0-SNAPSHOT.jar

# Phase 2
flink run -c com.example.flighttrack.phase2.WindowAggregationJob target/flight-track-1.0-SNAPSHOT.jar

# Phase 3
flink run -c com.example.flighttrack.phase3.StatefulDetectionJob target/flight-track-1.0-SNAPSHOT.jar
```

**Configuration:** Job parameters (file paths, window sizes, thresholds) are currently hardcoded in each job class. Future work will externalize these to `flink-conf.yaml` or runtime parameters.

## Phase 2 Details

Phase 2 runs in **STREAMING** execution mode (not BATCH) to preserve watermark mechanics for educational purposes. The job produces four separate outputs:

### Exercise 2-A: Average Ground Speed
- **Window Type:** Tumbling (non-overlapping)
- **Window Size:** 5 minutes
- **Output:** Average ground speed per aircraft per window
- **Side Effect:** Captures late records for Exercise 2-D

### Exercise 2-B: Peak Altitude
- **Window Type:** Sliding (overlapping)
- **Window Size:** 10 minutes
- **Slide Interval:** 2 minutes
- **Overlap Factor:** Each record appears in up to 5 windows
- **Output:** Maximum altitude per aircraft per window

### Exercise 2-C: Flight Session Detection
- **Window Type:** Session (dynamic boundaries)
- **Gap Threshold:** 30 minutes
- **Behavior:** A session closes when no ping is received for 30 consecutive minutes in event time
- **Merge Logic:** Sessions merge if a late record bridges their gap
- **Use Case:** Distinguish between separate flights (e.g., aircraft with layover will produce 2 sessions)

### Exercise 2-D: Late Record Analysis
- **Mechanism:** Side output via `OutputTag<AircraftRecord>`
- **Captured From:** Exercise 2-A tumbling window (shared watermark applies to all windows)
- **Annotation:** Each late record is tagged with:
  - `eventTimeMs`: record's original timestamp
  - `latenessMs`: `currentWatermark - eventTimeMs`
- **Purpose:** Watermark calibration tuning (current tolerance: 15 minutes bounded out-of-orderness)

**Post-Run Analysis Commands:**
```bash
# Count late records
wc -l data/phase2-out/late-records/*

# Find maximum lateness (requires jq)
cat data/phase2-out/late-records/* | jq 'max_by(.latenessMs) | .latenessMs'

# Calculate late record percentage
total=$(wc -l < data/phase2-out/avg-speed/*)
late=$(wc -l < data/phase2-out/late-records/*)
echo "scale=2; $late / $total * 100" | bc
```

**Tuning Guideline:** If late % > 5%, increase watermark tolerance. If late % = 0%, tolerance may be overly conservative.

## Testing

Unit tests cover:
- JSON parsing with malformed/edge-case inputs
- Validation logic for all constraint boundaries
- Window aggregation correctness
- Anomaly detection state transitions

Run tests:
```bash
mvn test
```

## Data Format

**Input:** ADS-B exchange JSON snapshots

```json
{
  "now": 1772323199.001,
  "messages": 3303995385,
  "aircraft": [
    {
      "hex": "a12b3c",
      "flight": "UAL123  ",
      "alt_baro": 35000,
      "gs": 485.2,
      "track": 87.5,
      "lat": 37.7749,
      "lon": -122.4194,
      "squawk": "1200",
      "category": "A3",
      "r": "N123UA",
      "t": "B77W"
    }
  ]
}
```

- `now`: Unix epoch timestamp (seconds)
- `aircraft`: Array of position reports
- Fields use snake_case (parser maps to camelCase Kotlin properties)
- `alt_baro` may be the string `"ground"` for surface aircraft

## License

_(Not specified in provided code)_

## Contributing

_(Not specified in provided code)_