package com.example.flighttrack.phase1

/**
 * ⚠️  LEGACY — retained for reference only.
 *
 * [AdsbFileSource] was the original Phase 1 source, implemented as a Flink
 * [org.apache.flink.streaming.api.functions.source.SourceFunction].
 *
 * ## Why it was replaced
 * `SourceFunction` is **inherently unbounded**: Flink has no way to know at
 * graph-build time that `run()` will eventually return.  Combining an unbounded
 * source with `RuntimeExecutionMode.BATCH` therefore throws:
 *
 *   "Detected an UNBOUNDED source with 'execution.runtime-mode' set to 'BATCH'"
 *
 * ## Replacement
 * Use [AdsbSnapshotFormat] with Flink's [org.apache.flink.connector.file.src.FileSource]:
 *
 * ```kotlin
 * val source = FileSource
 *     .forRecordStreamFormat(AdsbSnapshotFormat(), Path(inputDir))
 *     .build()                              // ← BOUNDED by default
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "ADS-B File Source")
 * ```
 *
 * `FileSource` without `.monitorContinuously(…)` is BOUNDED, fully compatible
 * with BATCH mode, and is the recommended approach from Flink 1.14 onwards.
 *
 * `SourceFunction` is deprecated as of Flink 1.17.
 */
@Deprecated(
    message = "Use FileSource + AdsbSnapshotFormat instead. " +
              "SourceFunction is unbounded and incompatible with BATCH runtime mode.",
    replaceWith = ReplaceWith(
        "FileSource.forRecordStreamFormat(AdsbSnapshotFormat(), path).build()",
        "org.apache.flink.connector.file.src.FileSource",
    ),
)
class AdsbFileSource
