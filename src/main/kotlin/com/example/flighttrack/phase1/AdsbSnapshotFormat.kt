package com.example.flighttrack.phase1

import com.example.flighttrack.model.AircraftRecord
import com.example.flighttrack.parse.AircraftRecordParser
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat
import org.apache.flink.connector.file.src.reader.StreamFormat
import org.apache.flink.core.fs.FSDataInputStream

/**
 * Flink [SimpleStreamFormat] that reads one ADS-B snapshot JSON file
 * and emits individual [AircraftRecord] objects.
 *
 * ## Why SimpleStreamFormat fixes the BATCH mode error
 * The old [AdsbFileSource] extended `SourceFunction`, which is **inherently UNBOUNDED**:
 * Flink has no way to know at graph-build time that `run()` will eventually return.
 * That caused:
 *
 *   "Detected an UNBOUNDED source with 'execution.runtime-mode' set to 'BATCH'"
 *
 * [SimpleStreamFormat] plugs into [org.apache.flink.connector.file.src.FileSource], which
 * is **BOUNDED by default** (no `.monitorContinuously(…)` call).  Build the source with:
 *
 * ```kotlin
 * val source = FileSource
 *     .forRecordStreamFormat(AdsbSnapshotFormat(), Path(inputDir))
 *     .build()                               // ← BOUNDED
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "ADS-B File Source")
 * ```
 *
 * ## Non-splittable
 * JSON cannot be split at arbitrary byte boundaries, so [isSplittable] returns `false`
 * (the default from [SimpleStreamFormat]).  Each file is processed by a single task slot.
 *
 * ## Eager parsing
 * Each snapshot file is small (≤ a few MB for a 5-second ADS-B window), so the entire
 * aircraft list is parsed eagerly in [createReader] and held in memory as a [List].
 * The returned [StreamFormat.Reader] iterates over that list.
 */
class AdsbSnapshotFormat : SimpleStreamFormat<AircraftRecord>() {

    companion object {
        private const val serialVersionUID = 1L
    }

    // ── SimpleStreamFormat contract ───────────────────────────────────────────

    override fun getProducedType(): TypeInformation<AircraftRecord> =
        TypeInformation.of(AircraftRecord::class.java)

    /**
     * Called once per file split.  Detects compression, parses the snapshot,
     * and returns a [StreamFormat.Reader] backed by the resulting in-memory list.
     *
     * @param config Flink task configuration (unused here).
     * @param stream The raw bytes of the file split provided by [FileSource].
     */
    override fun createReader(
        config: Configuration,
        stream: FSDataInputStream,
    ): StreamFormat.Reader<AircraftRecord> {
        // ── Parse the snapshot eagerly ────────────────────────────────────────
        //
        // AircraftRecordParser reads the outer {"now":…,"aircraft":[…]} wrapper,
        // stamps each AircraftRecord with snapshotTime, and returns the list.
        val records: List<AircraftRecord> = AircraftRecordParser().parseSnapshot(stream)

        return ListReader(records)
    }

    // ── Inner Reader implementation ───────────────────────────────────────────

    /**
     * A [StreamFormat.Reader] that iterates over an in-memory [List].
     *
     * Flink calls [read] repeatedly until it returns `null`, at which point the
     * split is considered exhausted — making the source BOUNDED.
     */
    private class ListReader(
        records: List<AircraftRecord>,
    ) : StreamFormat.Reader<AircraftRecord> {

        private val iterator: Iterator<AircraftRecord> = records.iterator()

        /** Returns the next record, or `null` when the list is exhausted (end of file). */
        override fun read(): AircraftRecord? =
            if (iterator.hasNext()) iterator.next() else null

        /** Nothing to release — the backing list is garbage-collected normally. */
        override fun close(): Unit = Unit
    }
}
