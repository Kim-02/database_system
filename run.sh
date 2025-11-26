#!/bin/bash

for i in {1..5}; do
  echo "==== RUN $i ====" | tee -a AR3log_warm_M256_B4096_16384.log
  ./main.out 256 \
    part.tbl "partkey,name,mfgr,brand,type,size,container,retailprice,comment" 4096 \
    partsupp.tbl "partkey,suppkey,availqty,supplycost,comment" 16384 \
    partkey | tee -a AR3log_warm_M256_B4096_16384.log
done
