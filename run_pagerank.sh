#!/bin/bash

INPUT=/user/hadoop/input/graph.txt
OUTPUT_BASE=/user/hadoop/output

# xóa output cũ nếu có
hdfs dfs -rm -r -f $OUTPUT_BASE

# chạy lặp PageRank 10 lần
for i in {1..10}
do
  echo "Iteration $i"
  
  if [ $i -eq 1 ]; then
    IN=$INPUT
  else
    IN=${OUTPUT_BASE}_$((i - 1))
  fi

  OUT=${OUTPUT_BASE}_$i

  hadoop jar pagerank.jar pagerank.PageRankDriver $IN $OUT
done

echo "Done! Final output is in $OUT"

