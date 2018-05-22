#!/usr/bin/env bash

jar=out/spark-scala-kmeans.jar
in_dir=out/resources
out_dir=out/out_dir
partial_command="java -jar ${jar} ${in_dir}/0.45MP.png"

${partial_command} ${out_dir}/out-pmkpp-mr 30 8 -s parallelkmeans++ -p mapreduce
${partial_command} ${out_dir}/out-kmpp-mr 30 8 -s kmeans++ -p mapreduce
${partial_command} ${out_dir}/out-rand-mr 30 8 -s random -p mapreduce

${partial_command} ${out_dir}/out-kmpp-is 30 8 -s kmeans++ -p imagesplit
${partial_command} ${out_dir}/out-rand-is 30 8 -s random -p imagesplit

${partial_command} ${out_dir}/out-pkmpp-seq 30 8 -s parallelkmeans++
${partial_command} ${out_dir}/out-kmpp-seq 30 8 -s kmeans++
${partial_command} ${out_dir}/out-rand-seq 30 8 -s random