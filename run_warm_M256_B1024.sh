#!/bin/bash

for i in {1..5}; do
  echo "==== RUN $i ====" | tee -a warm_M256_B1024.log
  ./main.out 256 \
    part.tbl "partkey,name,mfgr,brand,type,size,container,retailprice,comment" 1024 \
    partsupp.tbl "partkey,suppkey,availqty,supplycost,comment" 1024 \
    partkey | tee -a warm_M256_B1024.log
done
