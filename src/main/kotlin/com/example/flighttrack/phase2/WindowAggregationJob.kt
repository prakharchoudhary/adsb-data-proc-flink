package com.example.flighttrack.phase2

import com.example.flighttrack.model.AircraftRecord
import com.example.flighttrack.model.FlightSegment
import com.example.flighttrack.parse.FieldValidator
import com.example.flighttrack.phase1.AdsbSnapshotFormat
import com.example.flighttrack.phase1.IngestFilterJob
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.time.Duration

/**
 * Phase 2 – Time-Based Aggregation (all four exercises)
 *
 * **Exercise 2-A – Average ground speed (tumbling 5-minute window)**
 *   Non-overlapping 5-minute intervals; one FlightSegment per (aircraft, window).
 *
 * **Exercise 2-B – Peak altitude (sliding 10-minute / 2-minute window)**
 *   Overlapping windows; each record belongs to up to 5 windows.
 *
 * **Exercise 2-C – Flight session detection (session window, 30-minute gap)**
 *   A session closes when no ping is heard for 30 consecutive minutes.
 *   One aircraft that lands, sits silent, and departs will produce 2 sessions.
 *
 * **Exercise 2-D – Capture late records via side output**
 *   Records whose event time falls below the current watermark are "late".
 *   Rather than dropping them silently, we route them to a dedicated side
 *   output, annotate each one with how many milliseconds late it arrived,
 *   and write them to a separate sink for post-run inspection.
 *
 * ── Why STREAMING mode ────────────────────────────────────────────────────────
 * BATCH mode short-circuits watermark propagation using the end-of-input
 * boundary, hiding how watermarks actually work. STREAMING mode lets you
 * observe the real mechanics: windows fire when the watermark crosses their
 * end time, and late records are detectable. Use STREAMING here for learning.
 */
class WindowAggregationJob {

    companion object {

        @JvmStatic
        fun executeJob(
            env: StreamExecutionEnvironment,
            inputDir: String  = "./data/raw",
            outputDir: String = "./data/phase2-out",
        ) {

            // ── Source + parse/filter ──────────────────────────────────────────

            val source = FileSource
                .forRecordStreamFormat(AdsbSnapshotFormat(), Path(inputDir))
                .build()

            val airborne = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "ADS-B File Source ($inputDir)")
                .filter { FieldValidator.validate(it).valid }
                .name("Implausibility Filter")
                .filter { it.altBaro != null && it.altBaro!! >= IngestFilterJob.MIN_AIRBORNE_ALT_FT }
                .name("Airborne Filter (altBaro ≥ ${IngestFilterJob.MIN_AIRBORNE_ALT_FT} ft)")

            // ── Event-time watermark assignment ───────────────────────────────
            //
            // snapshotTime is Double SECONDS; Flink's time system uses Long MILLISECONDS.
            // (snapshotTime * 1000).toLong() is the required conversion.
            // Forgetting this makes every window 1000× wider than intended.
            //
            // Watermarks are assigned on the pre-partition stream so the clock
            // advances uniformly regardless of which aircraft produce records.

            val timedStream = airborne.assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness<AircraftRecord>(Duration.ofMinutes(15))
                    .withTimestampAssigner { record, _ -> (record.snapshotTime * 1000).toLong() }
            ).name("Event-Time Watermark (5 s tolerance)")

            val keyed = timedStream.keyBy { it.hex }

            // ── Exercise 2-D: OutputTag for late records ───────────────────────
            //
            // An OutputTag is a typed label for a secondary output channel.
            // It must be defined before any window that uses it.
            // The generic type parameter must match the stream element type
            // (AircraftRecord here, not FlightSegment — late records are raw
            // inputs that never made it into a window result).
            //
            // We attach this tag to the 2-A tumbling window.  Because all three
            // windows share the same watermark (derived from the same timedStream),
            // records that are late for 2-A are equally late for 2-B and 2-C.
            // One side output is sufficient to measure watermark calibration.

            // OutputTag MUST be an anonymous inner class (object : ... {}) so the JVM
            // can capture the generic type parameter at compile time.  If you write
            //   OutputTag<AircraftRecord>("late-records")          ← WRONG
            // the JVM erases the generic type at runtime and Flink cannot determine
            // the TypeInformation it needs to serialise side-output records.
            // The anonymous class form preserves the type token:
            //   object : OutputTag<AircraftRecord>("late-records") {}  ← CORRECT
            val lateTag = object : OutputTag<AircraftRecord>("late-records") {}

            // ── Exercise 2-A: Average ground speed – tumbling 5-minute window ──
            //
            // .sideOutputLateData(lateTag) is added here (Exercise 2-D).
            // It does not change the main output — it only opens a secondary
            // channel that Flink fills whenever a record arrives after its window
            // has already been closed by the watermark.
            //
            // The return type of .aggregate() is SingleOutputStreamOperator, NOT
            // DataStream.  We keep the reference as avgSpeedOp (not avgSpeedStream)
            // so we can call .getSideOutput(lateTag) on it for 2-D below.

            val avgSpeedOp = keyed
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sideOutputLateData(lateTag)
                .aggregate(AvgSpeedAggregateFunction(), SpeedWindowFunction())
                .name("Ex 2-A: Avg Speed | Tumbling 5-min")

            // ── Exercise 2-B: Peak altitude – sliding 10-minute / 2-minute ─────
            //
            // size=10 min, slide=2 min → each record belongs to up to 5 windows.
            // No side output here; lateness is measured through 2-A's tag.

            val peakAltOp = keyed
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(2)))
                .aggregate(PeakAltAggregateFunction(), AltWindowFunction())
                .name("Ex 2-B: Peak Altitude | Sliding 10-min/2-min")

            // ── Exercise 2-C: Flight session detection – session window ─────────
            //
            // EventTimeSessionWindows.withGap(30 min) is fundamentally different
            // from tumbling/sliding windows:
            //
            //   1. No fixed boundaries.  A session starts with the first ping and
            //      stays open as long as pings keep arriving.
            //
            //   2. A session CLOSES when no ping is seen for the gap duration
            //      (30 min) in event time — i.e. when the watermark exceeds
            //      (last-ping-time + 30 min).
            //
            //   3. Sessions can MERGE.  If two separate sessions for the same
            //      aircraft turn out to overlap (because a late record bridges
            //      their gap), Flink merges them into one session and calls
            //      SessionAggregateFunction.merge() to combine their accumulators.
            //      This is why merge() is non-trivial for session windows — unlike
            //      tumbling/sliding where Flink never calls it.
            //
            // Verification: find a known long-haul flight (LHR–JFK).
            //   windowEndTime − windowStartTime should be ≈ 6–8 hours.
            //   An aircraft with two flights separated by a layover should appear
            //   as two distinct sessions with a gap > 30 min between them.

            val sessionOp = keyed
                .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
                .aggregate(SessionAggregateFunction(), SessionWindowFunction())
                .name("Ex 2-C: Flight Sessions | Session 30-min gap")

            // ── Exercise 2-D: Retrieve and annotate late records ──────────────
            //
            // getSideOutput(lateTag) extracts the secondary channel from the 2-A
            // window operator.  The stream contains raw AircraftRecord objects —
            // every record that arrived after the watermark had already passed
            // its 5-minute window's end time.
            //
            // LateRecordTagger wraps each record with:
            //   - eventTimeMs  : the record's own timestamp in milliseconds
            //   - latenessMs   : how far past the watermark it arrived
            //                    = currentWatermark − eventTimeMs
            //
            // Post-run analysis (run these after the job finishes):
            //   total records  : count lines in avg-speed/ output
            //   late count     : count lines in late-records/ output
            //   late %         : late / total × 100
            //   max lateness   : jq 'max_by(.latenessMs) | .latenessMs' on late output
            //
            // Rule of thumb: if late% > 5%, increase the watermark tolerance.
            // If late% = 0%, your tolerance is probably too generous for real data.

            val lateStream = avgSpeedOp
                .getSideOutput(lateTag)
                .process(LateRecordTagger())
                .name("Ex 2-D: Annotate Late Records")

            // ── Sinks ──────────────────────────────────────────────────────────

            avgSpeedOp
                .map(SegmentToJsonMapper())
                .name("Serialize 2-A")
                .sinkTo(fileSink("$outputDir/avg-speed"))
                .name("Ex 2-A: NDJSON FileSink")

            peakAltOp
                .map(SegmentToJsonMapper())
                .name("Serialize 2-B")
                .sinkTo(fileSink("$outputDir/peak-alt"))
                .name("Ex 2-B: NDJSON FileSink")

            sessionOp
                .map(SegmentToJsonMapper())
                .name("Serialize 2-C")
                .sinkTo(fileSink("$outputDir/sessions"))
                .name("Ex 2-C: NDJSON FileSink")

            lateStream
                .map(LateRecordToJsonMapper())
                .name("Serialize 2-D")
                .sinkTo(fileSink("$outputDir/late-records"))
                .name("Ex 2-D: NDJSON FileSink")
        }

        /** Convenience factory — avoids repeating FileSink boilerplate for each output. */
        private fun fileSink(path: String): FileSink<String> =
            FileSink.forRowFormat(Path(path), SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Accumulators
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Intermediate state for Exercise 2-A (average ground speed, tumbling window).
 *
 * [speedCount] is separate from [recordCount] because a valid airborne record
 * can carry altitude without carrying ground speed.  Dividing speedSum by
 * recordCount would silently undercount the average.
 */
data class SpeedAccumulator(
    var hex: String       = "",
    var flight: String?   = null,
    var recordCount: Int  = 0,
    var speedSum: Double  = 0.0,
    var speedCount: Int   = 0,
    var minSpeed: Double? = null,
    var maxSpeed: Double? = null,
    var minAlt: Int?      = null,
    var maxAlt: Int?      = null,
)

/** Intermediate state for Exercise 2-B (peak altitude, sliding window). */
data class AltAccumulator(
    var hex: String      = "",
    var flight: String?  = null,
    var recordCount: Int = 0,
    var minAlt: Int?     = null,
    var maxAlt: Int?     = null,
)

/**
 * Intermediate state for Exercise 2-C (flight session, session window).
 *
 * [firstPingTime] and [lastPingTime] track the actual earliest and latest
 * snapshotTime (seconds) within the session.  We store them in the accumulator
 * rather than deriving from [context.window()] because:
 *   - window().start is the first assigned timestamp, which matches firstPingTime.
 *   - window().end is lastPingTime + gapDuration (30 min), NOT the last ping.
 *
 * Using accumulator-tracked values gives us the true session boundaries.
 *
 * [Double.MAX_VALUE] as the initial [firstPingTime] is a sentinel meaning
 * "no records yet".  It is overwritten by the first call to add().
 */
data class SessionAccumulator(
    var hex: String          = "",
    var flight: String?      = null,
    var recordCount: Int     = 0,
    var firstPingTime: Double = Double.MAX_VALUE,
    var lastPingTime: Double  = 0.0,
)

/**
 * A late AircraftRecord annotated with how many milliseconds after the
 * watermark it arrived.  Produced by [LateRecordTagger] (Exercise 2-D).
 */
data class LateRecord(
    val hex: String,
    val flight: String?,
    val snapshotTime: Double,
    val eventTimeMs: Long,
    val latenessMs: Long,
    val altBaro: Int?,
    val groundSpeed: Double?,
)

// ─────────────────────────────────────────────────────────────────────────────
// AggregateFunction implementations
// ─────────────────────────────────────────────────────────────────────────────

/** Accumulates speed/altitude statistics for Exercise 2-A. */
class AvgSpeedAggregateFunction : AggregateFunction<AircraftRecord, SpeedAccumulator, SpeedAccumulator> {

    override fun createAccumulator() = SpeedAccumulator()

    override fun add(record: AircraftRecord, acc: SpeedAccumulator): SpeedAccumulator {
        if (acc.hex.isEmpty()) acc.hex = record.hex
        if (acc.flight == null && record.flight != null) acc.flight = record.flight
        acc.recordCount++

        record.groundSpeed?.let { gs ->
            acc.speedSum   += gs
            acc.speedCount++
            acc.minSpeed    = minOf(acc.minSpeed ?: gs, gs)
            acc.maxSpeed    = maxOf(acc.maxSpeed ?: gs, gs)
        }
        record.altBaro?.let { alt ->
            acc.minAlt = minOf(acc.minAlt ?: alt, alt)
            acc.maxAlt = maxOf(acc.maxAlt ?: alt, alt)
        }
        return acc
    }

    override fun getResult(acc: SpeedAccumulator): SpeedAccumulator = acc

    // merge() is only called by Flink for session windows; included here for
    // correctness but never invoked by 2-A's tumbling window.
    override fun merge(a: SpeedAccumulator, b: SpeedAccumulator) = SpeedAccumulator(
        hex         = a.hex.ifEmpty { b.hex },
        flight      = a.flight ?: b.flight,
        recordCount = a.recordCount + b.recordCount,
        speedSum    = a.speedSum    + b.speedSum,
        speedCount  = a.speedCount  + b.speedCount,
        minSpeed    = listOfNotNull(a.minSpeed, b.minSpeed).minOrNull(),
        maxSpeed    = listOfNotNull(a.maxSpeed, b.maxSpeed).maxOrNull(),
        minAlt      = listOfNotNull(a.minAlt,   b.minAlt).minOrNull(),
        maxAlt      = listOfNotNull(a.maxAlt,   b.maxAlt).maxOrNull(),
    )
}

/** Accumulates peak altitude for Exercise 2-B. */
class PeakAltAggregateFunction : AggregateFunction<AircraftRecord, AltAccumulator, AltAccumulator> {

    override fun createAccumulator() = AltAccumulator()

    override fun add(record: AircraftRecord, acc: AltAccumulator): AltAccumulator {
        if (acc.hex.isEmpty()) acc.hex = record.hex
        if (acc.flight == null && record.flight != null) acc.flight = record.flight
        acc.recordCount++
        record.altBaro?.let { alt ->
            acc.minAlt = minOf(acc.minAlt ?: alt, alt)
            acc.maxAlt = maxOf(acc.maxAlt ?: alt, alt)
        }
        return acc
    }

    override fun getResult(acc: AltAccumulator): AltAccumulator = acc

    override fun merge(a: AltAccumulator, b: AltAccumulator) = AltAccumulator(
        hex         = a.hex.ifEmpty { b.hex },
        flight      = a.flight ?: b.flight,
        recordCount = a.recordCount + b.recordCount,
        minAlt      = listOfNotNull(a.minAlt, b.minAlt).minOrNull(),
        maxAlt      = listOfNotNull(a.maxAlt, b.maxAlt).maxOrNull(),
    )
}

/**
 * Accumulates session-level statistics for Exercise 2-C.
 *
 * Unlike 2-A and 2-B, [merge] is CRITICAL here and will actually be called
 * at runtime.  Here is why:
 *
 * Flink assigns each incoming record to a session window [t, t + gap].
 * When two adjacent session windows for the same key overlap or touch,
 * Flink merges them into one.  At that point it calls merge(a, b) to
 * combine the accumulators of the two windows being unified.
 *
 * If merge() is wrong (e.g. drops firstPingTime from one side), the merged
 * session will have an incorrect start time even though the record count is
 * correct.  Always validate both: check that firstPingTime equals the
 * earliest ping in the session and lastPingTime equals the latest.
 */
class SessionAggregateFunction : AggregateFunction<AircraftRecord, SessionAccumulator, SessionAccumulator> {

    override fun createAccumulator() = SessionAccumulator()

    override fun add(record: AircraftRecord, acc: SessionAccumulator): SessionAccumulator {
        if (acc.hex.isEmpty()) acc.hex = record.hex
        if (acc.flight == null && record.flight != null) acc.flight = record.flight
        acc.recordCount++
        // minOf/maxOf against the sentinel defaults correctly on the first record
        acc.firstPingTime = minOf(acc.firstPingTime, record.snapshotTime)
        acc.lastPingTime  = maxOf(acc.lastPingTime,  record.snapshotTime)
        return acc
    }

    override fun getResult(acc: SessionAccumulator): SessionAccumulator = acc

    override fun merge(a: SessionAccumulator, b: SessionAccumulator) = SessionAccumulator(
        hex           = a.hex.ifEmpty { b.hex },
        flight        = a.flight ?: b.flight,
        recordCount   = a.recordCount   + b.recordCount,
        // Take the earlier of the two session starts and the later of the two ends
        firstPingTime = minOf(a.firstPingTime, b.firstPingTime),
        lastPingTime  = maxOf(a.lastPingTime,  b.lastPingTime),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// ProcessWindowFunction implementations
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Converts [SpeedAccumulator] → [FlightSegment], injecting window boundaries.
 *
 * When called via `.aggregate(aggFn, windowFn)`, [elements] always contains
 * exactly ONE item (the output of AggregateFunction.getResult).  Calling
 * elements.first() is correct and intentional — do not iterate the collection.
 *
 * Window timestamps are Long milliseconds; divide by 1000.0 to match
 * FlightSegment's Double-seconds convention.
 */
class SpeedWindowFunction : ProcessWindowFunction<SpeedAccumulator, FlightSegment, String, TimeWindow>() {

    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SpeedAccumulator>,
        out: Collector<FlightSegment>,
    ) {
        val acc      = elements.first()
        val avgSpeed = if (acc.speedCount > 0) acc.speedSum / acc.speedCount else null

        out.collect(FlightSegment(
            hex             = key,
            flight          = acc.flight,
            windowStartTime = context.window().start / 1000.0,
            windowEndTime   = context.window().end   / 1000.0,
            recordCount     = acc.recordCount,
            minAltBaro      = acc.minAlt,
            maxAltBaro      = acc.maxAlt,
            minGroundSpeed  = acc.minSpeed,
            maxGroundSpeed  = acc.maxSpeed,
            avgGroundSpeed  = avgSpeed,
            minLat = null, maxLat = null, minLon = null, maxLon = null,
            squawks = emptyList(), emergencies = emptyList(),
        ))
    }
}

/** Converts [AltAccumulator] → [FlightSegment] with window metadata (Exercise 2-B). */
class AltWindowFunction : ProcessWindowFunction<AltAccumulator, FlightSegment, String, TimeWindow>() {

    override fun process(
        key: String,
        context: Context,
        elements: Iterable<AltAccumulator>,
        out: Collector<FlightSegment>,
    ) {
        val acc = elements.first()

        out.collect(FlightSegment(
            hex             = key,
            flight          = acc.flight,
            windowStartTime = context.window().start / 1000.0,
            windowEndTime   = context.window().end   / 1000.0,
            recordCount     = acc.recordCount,
            minAltBaro      = acc.minAlt,
            maxAltBaro      = acc.maxAlt,
            minGroundSpeed  = null, maxGroundSpeed  = null, avgGroundSpeed  = null,
            minLat = null, maxLat = null, minLon = null, maxLon = null,
            squawks = emptyList(), emergencies = emptyList(),
        ))
    }
}

/**
 * Converts [SessionAccumulator] → [FlightSegment] for Exercise 2-C.
 *
 * Session window boundaries differ from tumbling/sliding:
 *   context.window().start  =  first-ping event time (ms)
 *   context.window().end    =  last-ping event time + gap duration (ms)
 *                           ≠  actual last ping time
 *
 * We use the accumulator-tracked [firstPingTime] / [lastPingTime] (in seconds)
 * for windowStartTime / windowEndTime so the output reflects actual ping times,
 * not the artificial window-end boundary that includes the 30-minute gap.
 *
 * Session duration in minutes = (windowEndTime - windowStartTime) / 60.0
 * (no dedicated field in FlightSegment; derive it when reading the output).
 */
class SessionWindowFunction : ProcessWindowFunction<SessionAccumulator, FlightSegment, String, TimeWindow>() {

    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SessionAccumulator>,
        out: Collector<FlightSegment>,
    ) {
        val acc = elements.first()

        // Guard: if firstPingTime is still MAX_VALUE the accumulator is empty
        // (can happen if all records in the session had null snapshotTime).
        // Emit nothing rather than a nonsensical segment.
        if (acc.firstPingTime == Double.MAX_VALUE) return

        out.collect(FlightSegment(
            hex             = key,
            flight          = acc.flight,
            // Use accumulator-tracked actual ping times, NOT window().start/end
            windowStartTime = acc.firstPingTime,
            windowEndTime   = acc.lastPingTime,
            recordCount     = acc.recordCount,
            minAltBaro      = null, maxAltBaro     = null,
            minGroundSpeed  = null, maxGroundSpeed = null, avgGroundSpeed = null,
            minLat = null, maxLat = null, minLon = null, maxLon = null,
            squawks = emptyList(), emergencies = emptyList(),
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Late record annotator (Exercise 2-D)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Wraps each late [AircraftRecord] with lateness metadata.
 *
 * Called on the side-output stream produced by the 2-A tumbling window.
 * By the time a record appears in this stream, the watermark has already
 * advanced past its window's end time.
 *
 * latenessMs = currentWatermark − eventTimeMs
 *
 * [context.timerService().currentWatermark()] returns the last watermark
 * received by this operator.  Because the side-output stream flows downstream
 * of the window operator, this watermark represents how far the clock had
 * advanced when the record was declared late — a good approximation of the
 * true lateness.
 *
 * Use latenessMs to answer: "By how much did this record miss its window?"
 *   e.g. latenessMs = 3200 means the record arrived 3.2 s after its window
 *   had already fired.
 */
class LateRecordTagger : ProcessFunction<AircraftRecord, LateRecord>() {

    override fun processElement(
        record: AircraftRecord,
        ctx: Context,
        out: Collector<LateRecord>,
    ) {
        val eventTimeMs  = (record.snapshotTime * 1000).toLong()
        // coerceAtLeast(0) guards against the edge case where the watermark
        // has not yet advanced past the record (should not happen in the late
        // side output, but defensive coding avoids negative lateness values).
        val latenessMs   = (ctx.timerService().currentWatermark() - eventTimeMs).coerceAtLeast(0L)

        out.collect(LateRecord(
            hex          = record.hex,
            flight       = record.flight,
            snapshotTime = record.snapshotTime,
            eventTimeMs  = eventTimeMs,
            latenessMs   = latenessMs,
            altBaro      = record.altBaro,
            groundSpeed  = record.groundSpeed,
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON serialisers
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Serialises a [FlightSegment] to a compact JSON string.
 * ObjectMapper is @Transient and created once per task slot in open()
 * to avoid Kryo serialisation issues and per-record instantiation overhead.
 */
private class SegmentToJsonMapper : RichMapFunction<FlightSegment, String>() {
    @Transient private lateinit var mapper: ObjectMapper
    override fun open(parameters: Configuration) { mapper = jacksonObjectMapper() }
    override fun map(segment: FlightSegment): String = mapper.writeValueAsString(segment)
}

/** Serialises a [LateRecord] to a compact JSON string. */
private class LateRecordToJsonMapper : RichMapFunction<LateRecord, String>() {
    @Transient private lateinit var mapper: ObjectMapper
    override fun open(parameters: Configuration) { mapper = jacksonObjectMapper() }
    override fun map(record: LateRecord): String = mapper.writeValueAsString(record)
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

fun main(args: Array<String>) {
    val inputDir  = args.getOrElse(0) { "./data/raw" }
    val outputDir = args.getOrElse(1) { "./data/phase2-out" }

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    WindowAggregationJob.executeJob(env, inputDir, outputDir)

    env.execute("Phase 2 – Time-Based Aggregation")
}
