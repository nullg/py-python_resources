#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./job_chain_driver.sh [place_file_location] [input_location] [output_location] [country (optional)]"
    exit 1
fi

countries=`cut -d" " -f4- <<< "$@"`

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar \
-D stream.num.map.output.key.fields=1 \
-D mapreduce.job.maps=1 \
-D mapreduce.job.reduces=0 \
-D mapreduce.job.name='Place filter' \
-file place_filter_mapper.py \
-mapper "place_filter_mapper.py $countries" \
-input $1 \
-output ""$3"tmpFile"

hdfs dfs -copyToLocal ""$3"tmpFile/part-00000"

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar \
-D stream.num.map.output.key.fields=1 \
-D mapreduce.job.reduces=0 \
-D mapreduce.job.name='Replicate join' \
-files part-00000 \
-file replicate_join_mapper.py \
-mapper replicate_join_mapper.py \
-input $2 \
-output $3

hdfs dfs -rm -r -f ""$3"tmpFile*"

rm -f part-00000
