#!/bin/bash

for i in {1..5}; do
  echo "==== RUN $i ====" | tee -a warm_M256_B2048.log
  ./main.out 256 \
    part.tbl "partkey,name,mfgr,brand,type,size,container,retailprice,comment" 2048 \
    partsupp.tbl "partkey,suppkey,availqty,supplycost,comment" 2048 \
    partkey | tee -a warm_M256_B2048.log
done
