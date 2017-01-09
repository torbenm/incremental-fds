@echo off

set filename="benchmark.adultfull.csv"

set test_versions=1
set batch_sizes=10 100 1000 10000
set tests_per_version_and_batch=3

for %%v in (%test_versions%) do (
    for /L %%r in (1,1,%tests_per_version_and_batch%) do (
        for %%i in (%batch_sizes%) do (
            echo "Running test for version %%v, run %%r and batchsize %%i"
            java -Xmx8g -jar benchmarks/target/benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar --name "Better sampling" --batchSize %%i --sampling
        )
    )
)