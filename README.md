![Plush](plush.png)

# Plush

For details, please refer to our VLDB paper "Plush: A Write-Optimized Persistent Log-Structured Hash-Table".

Reference forthcoming.

Copyright (c) 2022 TUM. All rights reserved.

## Requirements
AVX-512 support, a mounted PMem partition in fsdax mode,  g++ >= 11. Some of the other data structures also require pmdk.

Note: While we tried to make the data structure as generic as possible, 
some constants are specific to our personal pmem server setup.
You might need to change a few paths, e.g., the path to the pmem partition in `src/benchmarking/PiBenchWrapper.hpp`.


## Project Structure
Plush itself is implemented in `src/hashtable/hashtable.cpp` and `src/hashtable/hashtable.h`.

The target `hashtable` builds a dynamic library which can be included in any other program.
This library also already includes `PiBench` bindings and can thus be called natively 
by the [PiBench benchmarking framework](https://github.com/sfu-dis/pibench) by Lersch et al.


We include patched, PiBench-enabled versions of the data structures we compare against as git submodules.
To populate them, run `git submodule update --init --recursive`.


## Reproducing the Results of the Paper

We have added the raw benchmarking results used to generate the plots paper in the `scripts/benchmarks` subdirectory.


If you want to verify the results yourself, you can use the shell scripts in the `scripts` directory.
You can then use the R scripts in the `scripts/R` subdirectory to generate the figures as pdf and tex files by executing
`Rscript filename.R`.

To get Plush's performance without storing filters in DRAM, set the constant `MAX_DRAM_FILTER_LEVEL` in `hashtable.h` to `-1`.

## Examples

We have also added two small example programs to show how we intend Plush to be used. You can find them in the `examples` directory.
They're also cmake build targets.
To run them, please modify the paths according to your local machine's PMem mount location.

`example.cpp` shows all features of Plush in a minimal example.

`gutenberg.cpp` demonstrates Plush's support for variable sized records by loading and storing all ebooks
of _Project Gutenberg_. Note that this is quite slow as we only use a single thread and most time is spend
loading the books (naively) into main memory.


You can play around by changing all the constants in `hashtable.h`, but please note that not all constants can currently
be choosen freely. However, if initialization of Plush seems quite slow, you might want to play around with

    PAYLOAD_LOG_NUM_BITS
    PAYLOAD_CHUNK_NUM_BITS
    CHUNKS_PER_PAYLOAD_LOG
    PAYLOAD_CHUNK_SIZE
