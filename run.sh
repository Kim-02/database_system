#!/bin/bash
set -euo pipefail

RUNS=3

LEFT_FILE="lineitem.tbl"
LEFT_SCHEMA="orderkey,partkey,suppkey,linenumber,quantity,extendedprice,discount,tax,returnflag,linestatus,shipdate,commitdate,receiptdate,shipinstruct,shipmode,comment"
RIGHT_FILE="orders.tbl"
RIGHT_SCHEMA="orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,clerk,shippriority,comment"
KEY="orderkey"

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
avg_tc "M2_B1024" 2 1024 1024
avg_tc "M2_B2048" 2 2048 2048
avg_tc "M2_B4096" 2 4096 4096
avg_tc "M2_B8192" 2 8192 8192
avg_tc "M2_B16384" 2 16384 16384

avg_tc "M4_B1024" 4 1024 1024
avg_tc "M4_B2048" 4 2048 2048
avg_tc "M4_B4096" 4 4096 4096
avg_tc "M4_B8192" 4 8192 8192
avg_tc "M4_B16384" 4 16384 16384

avg_tc "M8_B1024" 8 1024 1024
avg_tc "M8_B2048" 8 2048 2048
avg_tc "M8_B4096" 8 4096 4096
avg_tc "M8_B8192" 8 8192 8192
avg_tc "M8_B16384" 8 16384 16384

avg_tc "M16_B1024" 16 1024 1024
avg_tc "M16_B2048" 16 2048 2048
avg_tc "M16_B4096" 16 4096 4096
avg_tc "M16_B8192" 16 8192 8192
avg_tc "M16_B16384" 16 16384 16384

avg_tc "M32_B1024" 32 1024 1024
avg_tc "M32_B2048" 32 2048 2048
avg_tc "M32_B4096" 32 4096 4096
avg_tc "M32_B8192" 32 8192 8192
avg_tc "M32_B16384" 32 16384 16384

avg_tc "M64_B1024" 64 1024 1024
avg_tc "M64_B2048" 64 2048 2048
avg_tc "M64_B4096" 64 4096 4096
avg_tc "M64_B8192" 64 8192 8192
avg_tc "M64_B16384" 64 16384 16384

avg_tc "M128_B1024" 128 1024 1024
avg_tc "M128_B2048" 128 2048 2048
avg_tc "M128_B4096" 128 4096 4096
avg_tc "M128_B8192" 128 8192 8192
avg_tc "M128_B16384" 128 16384 16384

avg_tc "M256_B1024" 256 1024 1024
avg_tc "M256_B2048" 256 2048 2048
avg_tc "M256_B4096" 256 4096 4096
avg_tc "M256_B8192" 256 8192 8192
avg_tc "M256_B16384" 256 16384 16384

avg_tc "M512_B1024" 512 1024 1024
avg_tc "M512_B2048" 512 2048 2048
avg_tc "M512_B4096" 512 4096 4096
avg_tc "M512_B8192" 512 8192 8192
avg_tc "M512_B16384" 512 16384 16384

# MM in {128,256}
# Case A: Left fixed (4096 or 8192), Right varies (1024..16384)
avg_tc "M128_L4096_R1024" 128 4096 1024
avg_tc "M128_L4096_R2048" 128 4096 2048
avg_tc "M128_L4096_R4096" 128 4096 4096
avg_tc "M128_L4096_R8192" 128 4096 8192
avg_tc "M128_L4096_R16384" 128 4096 16384
avg_tc "M128_L8192_R1024" 128 8192 1024
avg_tc "M128_L8192_R2048" 128 8192 2048
avg_tc "M128_L8192_R4096" 128 8192 4096
avg_tc "M128_L8192_R8192" 128 8192 8192
avg_tc "M128_L8192_R16384" 128 8192 16384

avg_tc "M256_L4096_R1024" 256 4096 1024
avg_tc "M256_L4096_R2048" 256 4096 2048
avg_tc "M256_L4096_R4096" 256 4096 4096
avg_tc "M256_L4096_R8192" 256 4096 8192
avg_tc "M256_L4096_R16384" 256 4096 16384
avg_tc "M256_L8192_R1024" 256 8192 1024
avg_tc "M256_L8192_R2048" 256 8192 2048
avg_tc "M256_L8192_R4096" 256 8192 4096
avg_tc "M256_L8192_R8192" 256 8192 8192
avg_tc "M256_L8192_R16384" 256 8192 16384


# Case B: Right fixed (4096 or 8192), Left varies (1024..16384)
avg_tc "M128_L1024_R4096" 128 1024 4096
avg_tc "M128_L2048_R4096" 128 2048 4096
avg_tc "M128_L4096_R4096" 128 4096 4096
avg_tc "M128_L8192_R4096" 128 8192 4096
avg_tc "M128_L16384_R4096" 128 16384 4096
avg_tc "M128_L1024_R8192" 128 1024 8192
avg_tc "M128_L2048_R8192" 128 2048 8192
avg_tc "M128_L4096_R8192" 128 4096 8192
avg_tc "M128_L8192_R8192" 128 8192 8192
avg_tc "M128_L16384_R8192" 128 16384 8192

avg_tc "M256_L1024_R4096" 256 1024 4096
avg_tc "M256_L2048_R4096" 256 2048 4096
avg_tc "M256_L4096_R4096" 256 4096 4096
avg_tc "M256_L8192_R4096" 256 8192 4096
avg_tc "M256_L16384_R4096" 256 16384 4096
avg_tc "M256_L1024_R8192" 256 1024 8192
avg_tc "M256_L2048_R8192" 256 2048 8192
avg_tc "M256_L4096_R8192" 256 4096 8192
avg_tc "M256_L8192_R8192" 256 8192 8192
avg_tc "M256_L16384_R8192" 256 16384 8192