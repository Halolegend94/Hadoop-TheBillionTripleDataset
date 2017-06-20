#!/bin/bash

if [ -d "output" ]; then
    rm -r output
fi
mkdir output
hdfs dfs -get /output1 output/out1
hdfs dfs -get /output2 output/out2
hdfs dfs -get /output3 output/out3
hdfs dfs -get /output4 output/out4
hdfs dfs -get /output5 output/out5
hdfs dfs -get /output6 output/out6 

#hdfs dfs -rm -r /output1 /output2 /output3 /output4 /output5 /output6