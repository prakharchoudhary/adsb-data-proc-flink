package com.example.flighttrack.phase1

import com.example.flighttrack.model.AircraftRecord
import com.example.flighttrack.parse.FieldValidator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy

/**
 * Phase 1 – Ingest, Parse, Filter
 *
 * Implements the four exercises from § 4.3 of the project specification:
 *
 * **Exercise 1-A – Parse raw records**
 *   [AdsbSnapshotFormat] (via [FileSource]) reads every snapshot file and emits individual
 *   [AircraftRecord] objects (one per aircraft per snapshot). Records with
 *   implausible field values are discarded by [FieldValidator].
 *
 * **Exercise 1-B – Filter to airborne records**
 *   Aircraft on the ground are excluded using a conservative, documented rule:
 *   a record is considered airborne when `altBaro ≥ MIN_AIRBORNE_ALT_FT`.
 *   Rationale:
 *   - When an aircraft reports "ground" the raw `alt_baro` field is the string
 *     `"ground"`, which [AircraftRecordParser] maps to `altBaro = null`.
 *   - Taxiing aircraft occasionally report a small positive altitude (<100 ft)
 *     due to barometric encoder lags or airport elevation.
 *   - The `onGround` flag is unreliable per § 3.3.
 *   Therefore: `altBaro != null && altBaro >= 100` is the reliable definition.
 *
 * **Exercise 1-C – Partition by aircraft**
 *   `keyBy { hex }` routes all records for the same ICAO address to the same
 *   parallel slot, a prerequisite for stateful processing in Phases 2 and 3.
 *
 * **Exercise 1-D – Write results to a file sink**
 *   Each surviving record is serialised as a JSON string and written via
 *   [FileSink] to an output directory. The on-checkpoint rolling policy keeps
 *   in-progress part files small and readable with `cat` / `jq`.
 */
class IngestFilterJob {

    companion object {

        /** Minimum barometric altitude (ft) for a record to be considered airborne. */
        const val MIN_AIRBORNE_ALT_FT = 100

        @JvmStatic
        fun executeJob(
            env: StreamExecutionEnvironment,
            inputDir: String  = "./data/raw",
            outputDir: String = "./data/phase1-out",
        ) {

            // ── Exercise 1-A: Source + Parse ──────────────────────────────────
            //
            // FileSource + AdsbSnapshotFormat replaces the old SourceFunction-based
            // AdsbFileSource.  Key difference: FileSource without .monitorContinuously()
            // is BOUNDED by default, which is required by BATCH runtime mode.
            //
            // AdsbSnapshotFormat reads plain .json snapshot files,
            // parses the outer {"now":…, "aircraft":[…]} wrapper, and emits
            // individual AircraftRecord POJOs — one per aircraft per snapshot.
            //
            // The downstream filter discards records that fail implausibility
            // checks (e.g. ground-speed > 800 kt, impossible coordinates).

            val source: FileSource<AircraftRecord> = FileSource
                .forRecordStreamFormat(AdsbSnapshotFormat(), Path(inputDir))
                .build()

            val parsed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "ADS-B File Source ($inputDir)")
                .filter { record -> FieldValidator.validate(record).valid }
                .name("Ex 1-A: Implausibility Filter")

            // ── Exercise 1-B: Filter to airborne records ──────────────────────
            //
            // Reliable airborne definition (see KDoc above):
            //   altBaro != null  →  raw field was not the string "ground"
            //   altBaro >= 100   →  above taxi / encoder-lag threshold

            val airborne = parsed
                .filter { record ->
                    record.altBaro != null && record.altBaro!! >= MIN_AIRBORNE_ALT_FT
                }
                .name("Ex 1-B: Airborne Filter (altBaro ≥ ${MIN_AIRBORNE_ALT_FT} ft)")

            // ── Exercise 1-C: Partition by aircraft ───────────────────────────
            //
            // keyBy on the ICAO hex address guarantees that all records for a
            // single aircraft are handled by the same parallel task. This is
            // the correct partition key for the stateful windows in Phases 2/3.

            val keyed: KeyedStream<AircraftRecord, String> = airborne
                .keyBy { record -> record.hex }

            // ── Exercise 1-D: File sink (NDJSON) ─────────────────────────────
            //
            // Each airborne record is serialised to a compact JSON string and
            // appended to part files in outputDir. The output is one record per
            // line (NDJSON), making it trivially queryable with:
            //   cat data/phase1-out/part-* | jq '.flight'
            //
            // OnCheckpointRollingPolicy commits part files at each checkpoint;
            // in BATCH mode Flink finalises them when the job ends.

            val sink: FileSink<String> = FileSink
                .forRowFormat(Path(outputDir), SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build()

            keyed
                .map(RecordToJsonMapper())
                .name("Ex 1-D: Serialize to JSON")
                .sinkTo(sink)
                .name("Ex 1-D: NDJSON FileSink ($outputDir)")
        }
    }
}

/**
 * Serialises an [AircraftRecord] to a compact JSON string.
 *
 * [ObjectMapper] is declared `@Transient` so that Flink's closure-cleaner does
 * not attempt to serialise it (Jackson's [ObjectMapper] is not [java.io.Serializable]).
 * It is created once in [open] when the operator is initialised on the task manager —
 * avoiding both the serialisation problem and the per-record instantiation anti-pattern
 * that a plain lambda with `jacksonObjectMapper()` inside `map { }` would cause.
 */
private class RecordToJsonMapper : RichMapFunction<AircraftRecord, String>() {

    @Transient
    private lateinit var mapper: ObjectMapper

    override fun open(parameters: Configuration) {
        mapper = jacksonObjectMapper()
    }

    override fun map(record: AircraftRecord): String = mapper.writeValueAsString(record)
}

// ── Entry point ───────────────────────────────────────────────────────────────

fun main(args: Array<String>) {
    val inputDir  = args.getOrElse(0) { "./data/raw" }
    val outputDir = args.getOrElse(1) { "./data/phase1-out" }

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // BATCH mode: the source is bounded (finite files), so Flink can optimise
    // the execution plan and FileSink finalises part files when the job ends
    // without needing an explicit checkpoint.
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    IngestFilterJob.executeJob(env, inputDir, outputDir)

    env.execute("Phase 1 – Ingest, Parse, Filter")
}
