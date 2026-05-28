#!/usr/bin/env bash
set -euo pipefail

threshold="${CORAL_TABLES_PERF_MAX_MEAN_SECONDS:-2.5}"
runs="${CORAL_TABLES_PERF_RUNS:-5}"
warmup="${CORAL_TABLES_PERF_WARMUP:-1}"
coral_bin="${CORAL_BIN:-target/release/coral}"
fake_github_token="${CORAL_TABLES_PERF_GITHUB_TOKEN:-coral-ci-fake-token}"
sql="select * from coral.tables"

if ! command -v hyperfine >/dev/null 2>&1; then
  echo "hyperfine is required for the coral.tables performance check" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to evaluate hyperfine JSON output" >&2
  exit 1
fi

case "$coral_bin" in
  /*) ;;
  *) coral_bin="$PWD/$coral_bin" ;;
esac

if [ ! -x "$coral_bin" ]; then
  echo "Coral binary is not executable: $coral_bin" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

config_dir="$tmp_dir/coral-config"
mkdir -p "$config_dir"
cat >"$config_dir/config.toml" <<'EOF'
[credentials]
storage = "file"
EOF
export CORAL_CONFIG_DIR="$config_dir"

source_add_log="$tmp_dir/source-add.log"
if ! GITHUB_TOKEN="$fake_github_token" "$coral_bin" source add github >"$source_add_log" 2>&1; then
  cat "$source_add_log"
  exit 1
fi

echo "Installed github source with fake credentials."
tail -n 20 "$source_add_log"

"$coral_bin" sql "$sql" >/dev/null

result_json="$tmp_dir/hyperfine.json"
benchmark_command="$(printf '%q' "$coral_bin") sql $(printf '%q' "$sql") > /dev/null"
hyperfine \
  --warmup "$warmup" \
  --runs "$runs" \
  --export-json "$result_json" \
  --command-name "coral tables" \
  "$benchmark_command"

python3 - "$result_json" "$threshold" <<'PY'
import json
import sys

result_path = sys.argv[1]
threshold = float(sys.argv[2])

with open(result_path, encoding="utf-8") as result_file:
    result = json.load(result_file)

mean = result["results"][0]["mean"]
stddev = result["results"][0]["stddev"]

print(f"coral.tables mean: {mean:.3f}s (stddev {stddev:.3f}s, threshold {threshold:.3f}s)")
if mean > threshold:
    print(
        f"Performance regression: mean {mean:.3f}s exceeds {threshold:.3f}s",
        file=sys.stderr,
    )
    sys.exit(1)
PY
