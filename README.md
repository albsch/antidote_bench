# Antidote Benchmarks

## Get Started

1. Configure benchmark parameters in `src/antidote_driver.erl`
2. Execute `make run`


# Generate Graphs

```
# ./R/latency.R <op> <csv-path> <image-path>
./R/latency.R txn current/txn_single.csv latency_txn_single.png

# ./R/throughput.R <op> <csv-path> <image-path>
./R/throughput.R txn tests/txn_single.csv throughput_txn_single.png
```


To install R and ggplot2 on ubuntu:

```
sudo apt install r-base

R

> install.packages("ggplot2")
> install.packages("dplyr")
> install.packages("scales")
> install.packages("lubridate")
```



