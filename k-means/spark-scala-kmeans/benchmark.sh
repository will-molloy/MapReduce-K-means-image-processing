#!/usr/bin/env bash

jar=out/spark-scala-kmeans.jar
in_dir=out/resources
out_dir=out/out_dir
k_repeat="50 1"
runs=30
partial_command="spark-submit --executor-memory 6g ${jar} ${in_dir}/10MP.jpg ${out_dir}/out-"

for i in `seq 1 ${runs}`;
do
echo "*************** RUN ${i}/${runs} ***************"
${partial_command}rand-seq ${k_repeat} -s random
${partial_command}kmpp-seq ${k_repeat} -s kmeans++
${partial_command}pkmpp-seq ${k_repeat} -s parallelkmeans++

${partial_command}rand-mr ${k_repeat} -s random -p mapreduce
${partial_command}kmpp-mr ${k_repeat} -s kmeans++ -p mapreduce
${partial_command}pmkpp-mr ${k_repeat} -s parallelkmeans++ -p mapreduce
done
