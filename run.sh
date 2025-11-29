#!/bin/bash
set -euo pipefail

RUNS=1

LEFT_FILE="part.tbl"
LEFT_SCHEMA="partkey,name,mfgr,brand,type,size,container,retailprice,comment"
RIGHT_FILE="partsupp.tbl"
RIGHT_SCHEMA="partkey,suppkey,availqty,supplycost,comment"
KEY="partkey"

ORIG_DIR="$(pwd)"

abspath() {
  if command -v realpath >/dev/null 2>&1; then
    realpath "$1"
  else
    readlink -f "$1"
  fi
}

MAIN_BIN="$(abspath "$ORIG_DIR/main.out")"
LEFT_ABS="$(abspath "$LEFT_FILE")"
RIGHT_ABS="$(abspath "$RIGHT_FILE")"

SCRATCH_ROOT="$(mktemp -d -p "${TMPDIR:-/tmp}" "ar3_XXXXXX")"
cleanup_all() { rm -rf "$SCRATCH_ROOT"; }
trap cleanup_all EXIT

drop_caches() {
  if sudo -n true 2>/dev/null; then
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
  else
    echo "[WARN] cache drop skipped (need sudo). Run: sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
  fi
}

run_one() {
  local tc="$1" run_idx="$2" mm="$3" bl="$4" br="$5"
  local workdir
  workdir="$(mktemp -d -p "$SCRATCH_ROOT" "${tc}_run${run_idx}_XXXXXX")"

  (
    cd "$workdir"
    export TMPDIR="$workdir"
    "$MAIN_BIN" "$mm" \
      "$LEFT_ABS"  "$LEFT_SCHEMA"  "$bl" \
      "$RIGHT_ABS" "$RIGHT_SCHEMA" "$br" \
      "$KEY" 2>&1
  )
  rm -rf "$workdir"
}

extract_metric() {
  local out="$1" label="$2"
  echo "$out" | awk -F: -v pat="$label" '
    $0 ~ pat {
      gsub(/[[:space:]]*seconds.*/,"",$2);
      gsub(/[[:space:]]*/,"",$2);
      print $2; exit
    }'
}

avg_tc() {
  local tc="$1" mm="$2" bl="$3" br="$4"

  local log="$ORIG_DIR/AR3log_${tc}_MM${mm}.log"
  local results="$ORIG_DIR/results.tsv"

  local sum_elapsed=0 sum_read=0 sum_join=0 sum_write=0
  local last_out=""

  for i in $(seq 1 "$RUNS"); do
    echo "==== $tc RUN $i (MM=$mm B_L=$bl B_R=$br) ====" | tee -a "$log"
    drop_caches

    last_out="$(run_one "$tc" "$i" "$mm" "$bl" "$br")"
    echo "$last_out" | tee -a "$log"

    local elapsed read_t join_t write_t
    elapsed="$(extract_metric "$last_out" "Elapsed time")"
    read_t="$(extract_metric  "$last_out" "Read time")"
    join_t="$(extract_metric  "$last_out" "Join time")"
    write_t="$(extract_metric "$last_out" "Write time")"

    elapsed="${elapsed:-0}"; read_t="${read_t:-0}"; join_t="${join_t:-0}"; write_t="${write_t:-0}"

    sum_elapsed="$(awk -v a="$sum_elapsed" -v b="$elapsed" 'BEGIN{printf "%.12f", a+b}')"
    sum_read="$(awk    -v a="$sum_read"    -v b="$read_t"  'BEGIN{printf "%.12f", a+b}')"
    sum_join="$(awk    -v a="$sum_join"    -v b="$join_t"  'BEGIN{printf "%.12f", a+b}')"
    sum_write="$(awk   -v a="$sum_write"   -v b="$write_t" 'BEGIN{printf "%.12f", a+b}')"
  done

  local avg_elapsed avg_read avg_join avg_write
  avg_elapsed="$(awk -v s="$sum_elapsed" -v n="$RUNS" 'BEGIN{printf "%.6f", s/n}')"
  avg_read="$(awk    -v s="$sum_read"    -v n="$RUNS" 'BEGIN{printf "%.6f", s/n}')"
  avg_join="$(awk    -v s="$sum_join"    -v n="$RUNS" 'BEGIN{printf "%.6f", s/n}')"
  avg_write="$(awk   -v s="$sum_write"   -v n="$RUNS" 'BEGIN{printf "%.6f", s/n}')"

  # results.tsv는 누적 기록
  if [[ ! -f "$results" ]]; then
    echo -e "TC\tMM\tB_L\tB_R\tElapsed time\tRead time\tJoin time\tWrite time" > "$results"
  fi

  printf "%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\n" \
    "$tc" "$mm" "$bl" "$br" "$avg_elapsed" "$avg_read" "$avg_join" "$avg_write" | tee -a "$results"
}

# ---- 실행할 블록 (TC별 MM 지정) ----
avg_tc "MS1" 32  8192  8192
avg_tc "MS2" 64  8192  8192
avg_tc "MS3" 128  8192  8192
avg_tc "MS4" 256  8192  8192
avg_tc "MS5" 512  8192  8192

# 예시: TC별로 MM 바꾸려면 이렇게 추가
# avg_tc "M1" 128 4096 4096
# avg_tc "M2" 512 4096 4096
