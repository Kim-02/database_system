#!/bin/bash

# ============================================
# TPC-H Join Benchmark Script
# 사용법: ./benchmark.sh [메모리MB] [왼쪽블록] [오른쪽블록]
# 예시:   ./benchmark.sh 256 1024 2048
# ============================================

RUNS=5
MEM=${1:-256}
BLOCK_L=${2:-1024}
BLOCK_R=${3:-$BLOCK_L}  # 오른쪽 미지정시 왼쪽과 동일
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="benchmark_M${MEM}_BL${BLOCK_L}_BR${BLOCK_R}_${TIMESTAMP}.log"

echo "============================================" | tee $LOG_FILE
echo "  TPC-H Join Benchmark" | tee -a $LOG_FILE
echo "  Date: $(date)" | tee -a $LOG_FILE
echo "  Memory: ${MEM}MB, Left Block: ${BLOCK_L}B, Right Block: ${BLOCK_R}B, Runs: $RUNS" | tee -a $LOG_FILE
echo "============================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# 결과 저장용 배열
declare -a arr_elapsed
declare -a arr_read
declare -a arr_join
declare -a arr_write
declare -a arr_left_blocks
declare -a arr_right_blocks

for i in $(seq 1 $RUNS); do
    echo "-------------------- RUN $i --------------------" | tee -a $LOG_FILE
    
    # 실행하고 결과 캡처
    result=$(./main.out $MEM \
        part.tbl "partkey,name,mfgr,brand,type,size,container,retailprice,comment" $BLOCK_L \
        partsupp.tbl "partkey,suppkey,availqty,supplycost,comment" $BLOCK_R \
        partkey 2>&1)
    
    echo "$result" | tee -a $LOG_FILE
    
    # 각 항목 추출
    elapsed=$(echo "$result" | grep "Elapsed time" | awk '{print $4}')
    read_time=$(echo "$result" | grep "Read time" | awk '{print $4}')
    join_time=$(echo "$result" | grep "Join time" | awk '{print $4}')
    write_time=$(echo "$result" | grep "Write time" | awk '{print $4}')
    left_blocks=$(echo "$result" | grep "Left blocks" | awk '{print $4}')
    right_blocks=$(echo "$result" | grep "Right blocks" | awk '{print $4}')
    
    # 배열에 저장
    arr_elapsed+=($elapsed)
    arr_read+=($read_time)
    arr_join+=($join_time)
    arr_write+=($write_time)
    arr_left_blocks+=($left_blocks)
    arr_right_blocks+=($right_blocks)
    
    echo "" | tee -a $LOG_FILE
    
    # 실행 간 간격
    sleep 1
done

# 메모리 사용량 추출 - "Memory usage : 134217728 / 268435456 bytes (50.00%)"
mem_usage=$(echo "$result" | grep "Memory usage" | awk '{print $4}')
mem_limit=$(echo "$result" | grep "Memory usage" | awk '{print $6}')
mem_percent=$(echo "$result" | grep "Memory usage" | grep -o '([0-9.]*%)' | tr -d '()')

# 평균 계산 함수
calc_avg() {
    local arr=("$@")
    local sum=0
    local count=${#arr[@]}
    for val in "${arr[@]}"; do
        sum=$(echo "$sum + $val" | bc -l)
    done
    echo "scale=6; $sum / $count" | bc -l
}

# 평균 계산
avg_elapsed=$(calc_avg "${arr_elapsed[@]}")
avg_read=$(calc_avg "${arr_read[@]}")
avg_join=$(calc_avg "${arr_join[@]}")
avg_write=$(calc_avg "${arr_write[@]}")
avg_left=$(calc_avg "${arr_left_blocks[@]}")
avg_right=$(calc_avg "${arr_right_blocks[@]}")

# 결과 출력
echo "============================================" | tee -a $LOG_FILE
echo "  SUMMARY (Average of $RUNS runs)" | tee -a $LOG_FILE
echo "============================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

echo "[ Individual Results ]" | tee -a $LOG_FILE
echo "-----------------------------------------------" | tee -a $LOG_FILE
printf "%-6s %12s %12s %12s %12s %12s %12s\n" \
    "Run" "Elapsed" "Read" "Join" "Write" "Left_Blk" "Right_Blk" | tee -a $LOG_FILE
echo "-----------------------------------------------" | tee -a $LOG_FILE

for i in $(seq 0 $((RUNS-1))); do
    printf "%-6s %12.6f %12.6f %12.6f %12.6f %12d %12d\n" \
        "$((i+1))" \
        "${arr_elapsed[$i]}" \
        "${arr_read[$i]}" \
        "${arr_join[$i]}" \
        "${arr_write[$i]}" \
        "${arr_left_blocks[$i]}" \
        "${arr_right_blocks[$i]}" | tee -a $LOG_FILE
done

echo "-----------------------------------------------" | tee -a $LOG_FILE
printf "%-6s %12.6f %12.6f %12.6f %12.6f %12.0f %12.0f\n" \
    "AVG" "$avg_elapsed" "$avg_read" "$avg_join" "$avg_write" "$avg_left" "$avg_right" | tee -a $LOG_FILE
echo "-----------------------------------------------" | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "[ Parameters ]" | tee -a $LOG_FILE
echo "  Memory Limit    : ${MEM} MB" | tee -a $LOG_FILE
echo "  Left Block Size : ${BLOCK_L} B" | tee -a $LOG_FILE
echo "  Right Block Size: ${BLOCK_R} B" | tee -a $LOG_FILE
echo "  Memory Usage    : ${mem_usage:-N/A} / ${mem_limit:-N/A} bytes (${mem_percent:-N/A})" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "[ Time Breakdown (Average) ]" | tee -a $LOG_FILE
printf "  Elapsed Time    : %.6f seconds\n" "$avg_elapsed" | tee -a $LOG_FILE
printf "  Read Time       : %.6f seconds (%.1f%%)\n" "$avg_read" \
    $(echo "scale=1; $avg_read * 100 / $avg_elapsed" | bc -l) | tee -a $LOG_FILE
printf "  Join Time       : %.6f seconds (%.1f%%)\n" "$avg_join" \
    $(echo "scale=1; $avg_join * 100 / $avg_elapsed" | bc -l) | tee -a $LOG_FILE
printf "  Write Time      : %.6f seconds (%.1f%%)\n" "$avg_write" \
    $(echo "scale=1; $avg_write * 100 / $avg_elapsed" | bc -l) | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Log saved to: $LOG_FILE" | tee -a $LOG_FILE
echo "============================================" | tee -a $LOG_FILE
