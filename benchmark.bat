@echo off

set filename="benchmark.adultfull.csv"

set batch_sizes=100
set tests_per_version_and_batch=1

for /F "usebackq delims=" %%v in (benchmarks.txt) do (
    for /L %%r in (1,1,%tests_per_version_and_batch%) do (
        for %%i in (%batch_sizes%) do (
            echo "Running test for version %%v, run %%r and batchsize %%i"
            java -Xmx8g -jar benchmarks/target/benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar %%v --batchSize %%i
        )
    )
)