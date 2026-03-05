#!/usr/bin/env bash
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-1.18.1}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
INSTALL_ROOT="${INSTALL_ROOT:-$PWD/.tools}"
FLINK_DIST="flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}"
FLINK_HOME="${FLINK_HOME:-$INSTALL_ROOT/flink-${FLINK_VERSION}}"
FLINK_BIN="$FLINK_HOME/bin/flink"
JOB_CLASS="com.example.flighttrack.phase1.IngestFilterJobKt"
JAR_PATH_DEFAULT="target/flight-track-analytics-1.0-SNAPSHOT.jar"
UI_URL="${UI_URL:-http://localhost:8081}"
JOB_PARALLELISM="${JOB_PARALLELISM:-}"
JOB_PHASE="${JOB_PHASE:-phase1}"
JOB_CLASS_OVERRIDE="${JOB_CLASS_OVERRIDE:-}"

usage() {
  cat <<EOF
Usage: $0 <install|start|status|stop|run> [args]

Commands:
  install
    Download and extract Flink ${FLINK_VERSION} into: $INSTALL_ROOT

  start
    Install (if needed), start local Flink cluster, wait for UI

  status
    Check local Flink UI health at $UI_URL

  stop
    Stop local Flink cluster from $FLINK_HOME

  run [--phase phase1|phase2|phase3] [-p N|--parallelism N] [--class FQCN] [inputDir] [outputDir] [parallelism]
    Build jar and submit job to local cluster.
    Defaults:
      inputDir  = ./data/raw
      outputDir = ./data/<phase>-out/\$(date +%Y-%m-%d--%H)
      parallelism = cluster default (unless set via -p/--parallelism or JOB_PARALLELISM)
      phase = phase1
      class (by phase):
        phase1 -> com.example.flighttrack.phase1.IngestFilterJobKt
        phase2 -> com.example.flighttrack.phase2.WindowAggregationJobKt
        phase3 -> com.example.flighttrack.phase3.StatefulDetectionJob

    Examples:
      $0 run
      $0 run -p 8 ./data/raw ./data/phase1-out/run-p8
      $0 run --phase phase2 -p 4 ./data/raw ./data/phase2-out/run-p4
      $0 run --phase phase3 --class com.example.flighttrack.phase3.MyMainKt ./data/raw ./data/phase3-out/run
      JOB_PARALLELISM=6 $0 run ./data/raw ./data/phase1-out/run-p6

Environment overrides:
  FLINK_VERSION, SCALA_VERSION, INSTALL_ROOT, FLINK_HOME, UI_URL,
  JOB_PARALLELISM, JOB_PHASE, JOB_CLASS_OVERRIDE
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

download_flink() {
  local archive_name="${FLINK_DIST}.tgz"
  local tmp_archive="$INSTALL_ROOT/$archive_name"
  local primary_url="https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/${archive_name}"
  local backup_url="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${archive_name}"

  mkdir -p "$INSTALL_ROOT"
  echo "Downloading Flink from: $primary_url"
  if ! curl -fL "$primary_url" -o "$tmp_archive"; then
    echo "Primary mirror failed, trying archive.apache.org"
    curl -fL "$backup_url" -o "$tmp_archive"
  fi

  echo "Extracting $archive_name into $INSTALL_ROOT"
  tar -xzf "$tmp_archive" -C "$INSTALL_ROOT"

  # Resolve FLINK_HOME dynamically from extracted folder layout.
  if [[ -x "$INSTALL_ROOT/flink-${FLINK_VERSION}/bin/flink" ]]; then
    FLINK_HOME="$INSTALL_ROOT/flink-${FLINK_VERSION}"
  elif [[ -x "$INSTALL_ROOT/$FLINK_DIST/bin/flink" ]]; then
    FLINK_HOME="$INSTALL_ROOT/$FLINK_DIST"
  else
    local detected
    detected="$(find "$INSTALL_ROOT" -maxdepth 2 -type f -name flink -path "*/bin/flink" | head -n 1 || true)"
    if [[ -n "$detected" ]]; then
      FLINK_HOME="$(cd "$(dirname "$detected")/.." && pwd)"
    fi
  fi
  FLINK_BIN="$FLINK_HOME/bin/flink"

  rm -f "$tmp_archive"
}

install_flink() {
  require_cmd curl
  require_cmd tar
  require_cmd java

  if [[ -x "$FLINK_BIN" ]]; then
    echo "Flink already installed at: $FLINK_HOME"
    "$FLINK_BIN" --version || true
    return
  fi

  # If FLINK_HOME default is wrong, try to discover an existing extraction first.
  local detected
  detected="$(find "$INSTALL_ROOT" -maxdepth 2 -type f -name flink -path "*/bin/flink" | head -n 1 || true)"
  if [[ -n "$detected" ]]; then
    FLINK_HOME="$(cd "$(dirname "$detected")/.." && pwd)"
    FLINK_BIN="$FLINK_HOME/bin/flink"
    echo "Found existing Flink install at: $FLINK_HOME"
    "$FLINK_BIN" --version || true
    return
  fi

  download_flink
  if [[ ! -x "$FLINK_BIN" ]]; then
    echo "Installation failed; flink binary not found at $FLINK_BIN" >&2
    exit 1
  fi

  echo "Installed Flink at: $FLINK_HOME"
  "$FLINK_BIN" --version || true
}

wait_for_ui() {
  local max_wait_seconds=30
  local elapsed=0

  echo "Waiting for Flink UI at $UI_URL ..."
  while (( elapsed < max_wait_seconds )); do
    if curl -fsS "$UI_URL/overview" >/dev/null 2>&1; then
      echo "Flink UI is up: $UI_URL"
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  echo "Flink UI did not become ready within ${max_wait_seconds}s" >&2
  return 1
}

start_cluster() {
  install_flink
  "$FLINK_HOME/bin/start-cluster.sh"
  wait_for_ui
}

stop_cluster() {
  if [[ -x "$FLINK_HOME/bin/stop-cluster.sh" ]]; then
    "$FLINK_HOME/bin/stop-cluster.sh"
  else
    echo "No Flink install found at $FLINK_HOME"
  fi
}

status_cluster() {
  if curl -fsS "$UI_URL/overview" >/dev/null 2>&1; then
    echo "UP: Flink UI reachable at $UI_URL"
  else
    echo "DOWN: Flink UI not reachable at $UI_URL"
    return 1
  fi
}

run_job() {
  local input_dir="${1:-./data/raw}"
  local output_dir="${2:-}"
  local run_parallelism="${3:-}"
  local run_class="${4:-$JOB_CLASS}"
  local jar_path="$JAR_PATH_DEFAULT"

  require_cmd mvn
  install_flink
  status_cluster || {
    echo "Cluster is not running. Starting cluster..."
    start_cluster
  }

  echo "Building shaded jar..."
  mvn -DskipTests package

  if [[ ! -f "$jar_path" ]]; then
    echo "Jar not found: $jar_path" >&2
    exit 1
  fi

  require_cmd jar
  # Normalize potential CR characters from copied args/output.
  run_class="${run_class//$'\r'/}"
  local class_path
  class_path="$(printf '%s' "$run_class" | tr '.' '/')"
  class_path="${class_path}.class"
  local classes_list
  classes_list="$(jar tf "$jar_path" | tr -d '\r')"
  if ! grep -Fqx -- "$class_path" <<<"$classes_list"; then
    echo "Entry class not found in jar: $run_class" >&2
    echo "Expected class path: $class_path" >&2
    echo "Available matching classes:" >&2
    printf '%s\n' "$classes_list" | grep -F "com/example/flighttrack/phase" | grep -F "Job" >&2 || true
    echo "Use --class with a valid fully-qualified class name." >&2
    exit 1
  fi

  local run_args=()
  if [[ -n "$run_parallelism" ]]; then
    if [[ ! "$run_parallelism" =~ ^[1-9][0-9]*$ ]]; then
      echo "Invalid parallelism '$run_parallelism'. Use a positive integer." >&2
      exit 1
    fi
    run_args=(-p "$run_parallelism")
  fi

  echo "Submitting job..."
  "$FLINK_BIN" run \
    "${run_args[@]}" \
    -c "$run_class" \
    "$jar_path" \
    "$input_dir" \
    "$output_dir"

  echo "Submitted. Track at: $UI_URL"
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    install)
      install_flink
      ;;
    start)
      start_cluster
      ;;
    status)
      status_cluster
      ;;
    stop)
      stop_cluster
      ;;
    run)
      shift
      local run_phase="$JOB_PHASE"
      local run_parallelism="$JOB_PARALLELISM"
      local run_class="$JOB_CLASS_OVERRIDE"

      while [[ $# -gt 0 ]]; do
        case "${1:-}" in
          -p|--parallelism)
            run_parallelism="${2:-}"
            shift 2
            ;;
          --phase)
            run_phase="${2:-}"
            shift 2
            ;;
          --class)
            run_class="${2:-}"
            shift 2
            ;;
          --)
            shift
            break
            ;;
          *)
            break
            ;;
        esac
      done

      local default_output_dir
      case "$run_phase" in
        phase1)
          JOB_CLASS="com.example.flighttrack.phase1.IngestFilterJobKt"
          default_output_dir="./data/phase1-out/$(date +%Y-%m-%d--%H)"
          ;;
        phase2)
          JOB_CLASS="com.example.flighttrack.phase2.WindowAggregationJobKt"
          default_output_dir="./data/phase2-out/$(date +%Y-%m-%d--%H)"
          ;;
        phase3)
          JOB_CLASS="com.example.flighttrack.phase3.StatefulDetectionJob"
          default_output_dir="./data/phase3-out/$(date +%Y-%m-%d--%H)"
          ;;
        *)
          echo "Invalid phase '$run_phase'. Use: phase1, phase2, or phase3." >&2
          exit 1
          ;;
      esac

      if [[ -z "$run_class" ]]; then
        run_class="$JOB_CLASS"
      fi

      if [[ -z "$run_parallelism" && -n "${3:-}" ]]; then
        run_parallelism="${3}"
      fi
      run_job "${1:-./data/raw}" "${2:-$default_output_dir}" "$run_parallelism" "$run_class"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
