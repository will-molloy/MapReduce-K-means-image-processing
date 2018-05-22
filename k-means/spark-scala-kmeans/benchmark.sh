#!/usr/bin/env bash

jar=out/spark-scala-kmeans.jar
in_dir=out/resources
out_dir=out/out_dir
k_repeat="50 8"
log=SE751-Group9-MapReduce-scala-spark-kmeans.log
partial_command="spark-submit ${jar} ${in_dir}/10MP.jpg ${out_dir}/out-"

${partial_command}rand-seq ${k_repeat} -s random
${partial_command}kmpp-seq ${k_repeat} -s kmeans++
${partial_command}pkmpp-seq ${k_repeat} -s parallelkmeans++

${partial_command}rand-mr ${k_repeat} -s random -p mapreduce
${partial_command}kmpp-mr ${k_repeat} -s kmeans++ -p mapreduce
${partial_command}pmkpp-mr ${k_repeat} -s parallelkmeans++ -p mapreduce

${partial_command}rand-is ${k_repeat} -s random -p imagesplit
${partial_command}kmpp-is ${k_repeat} -s kmeans++ -p imagesplit

cat ${log}
