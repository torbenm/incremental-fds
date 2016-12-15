#!/bin/bash

filename = "benchmark.adultfull.csv"

test_versions=(0 1 2)
batch_sizes=(1 10 100 1000 10000 100000)
tests_per_version_and_batch=(1 2 3 4 5)
for version in ${test_versions[@]}; do
    for run in ${tests_per_version_and_batch[@]}; do
        for i in ${batch_sizes[@]}; do
            echo "Running test for version $version, run $run and batchsize $i"
             java -jar benchmarks/target/benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar $version $i
        done
    done
done
exit 0