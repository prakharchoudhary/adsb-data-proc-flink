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
  - Groups records by aircraft ICAO hex code
  - Aggregates within tumbling time windows
  - Computes min/max/avg statistics for altitude, speed, position
  - Collects distinct squawk codes and emergency statuses
- **Output:** `FlightSegment` summaries per window

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
│   ├── WindowAggregationJob.kt    # Phase 2 Flink job
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
```

## Building

```bash
mvn clean package
```

Produces a fat JAR suitable for Flink cluster deployment.

## Running

Each phase is a standalone Flink job. Submit to a Flink cluster:

```bash
# Phase 1
flink run -c com.example.flighttrack.phase1.IngestFilterJob target/flight-track-1.0-SNAPSHOT.jar

# Phase 2
flink run -c com.example.flighttrack.phase2.WindowAggregationJob target/flight-track-1.0-SNAPSHOT.jar

# Phase 3
flink run -c com.example.flighttrack.phase3.StatefulDetectionJob target/flight-track-1.0-SNAPSHOT.jar
```

**Configuration:** Job parameters (file paths, window sizes, thresholds) are currently hardcoded in each job class. Future work will externalize these to `flink-conf.yaml` or runtime parameters.

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