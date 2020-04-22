<h1 align="center">SHMR</h1>

A set of high-order map-reduce functions

<div align="center">

![PyPI](https://img.shields.io/pypi/v/shmr)
![Python](https://img.shields.io/badge/python-v3.6+-blue.svg)
[![GitHub Issues](https://img.shields.io/github/issues/binh-vu/shmr.svg)](https://github.com/binh-vu/shmr/issues)
![Contributions welcome](https://img.shields.io/badge/contributions-welcome-orange.svg)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

</div>

## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Examples](#examples)

## Introduction

The goal of this library is to make it easier to process large data in parallel while not spending lots of time writing code. The typical workflow of this library is to split a huge file into smaller partitions and process each partition in parallel as the map-reduce framework. However, it does not require any setup as Spark or Hadoop except for one simple `pip` installation command. It is more suitable for the research environment than for the production environment.

### Usage

This library, `shmr`, is best used in the command line with [`xargs`](https://en.wikipedia.org/wiki/Xargs) or [`parallel`](https://www.gnu.org/software/parallel/). You can see the list of supported arguments by printing help:

```bash
$ python -m shmr -h
usage: sh map-reduce (1.0.18) [-h] [-v] -i INFILE [--skip_nrows SKIP_NROWS]
                              [-d DESER_FN] [-s SER_FN] <command>
...
optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose
  -i INFILE, --infile INFILE
                        the path to one partition or list of partitions depend
                        on the sub-program
  --skip_nrows SKIP_NROWS
                        Skip first n rows of each partition
  -d DESER_FN, --deser_fn DESER_FN
                        Deserialization function. Default is `orjson.loads`
  -s SER_FN, --ser_fn SER_FN
                        Serialization function. Default is `orjson.dumps`
```

The most important argument is the positional argument `<command>`, which is the operator you want to run. There are two types of operator: the one that is applied on one partition, starts with `partition.*`, and the other one that is applied on all partitions, starts with `partitions.*`. The help command above will show you all of the possible commands, which is truncated for readability. You can get details of a command using help as well. For example:

```bash
$ python -m shmr partition.map -h
usage: sh map-reduce (1.0.18) partition.map [-h] --fn FN --outfile OUTFILE

Apply a map function on every record of this partition

optional arguments:
  -h, --help         show this help message and exit
  --fn FN            (str) an import path of the map function, which should
                     has this signature: (record: Any) -> Any
  --outfile OUTFILE  (str) output file of the new partition, `*` or `{stem}`
                     in the path are placeholders, which will be replaced by
                     the stem (i.e., file name without extension) of the
                     current partition
```

### Automatic partition naming

There are some variables you can use in naming the output partitions:

1. `{stem}`: will be replaced by the stem of the current mapping partition
2. `{auto}`: an incremental number of the new partition
3. `*` or `{}`: a special placeholder that is either `{stem}` or `{auto}`, depends on the function you are using. If the function generates multiple partitions (e.g., `group_by` function), then `*` or `{}` will be replaced by `{auto}`, otherwise, it will be replaced by `{stem}`. Note that `{stem}` of multiple partitions will always be replaced by an empty string

## Installation

From PyPi: `pip install shmr`

## Examples

Below are some examples:

1. Split one file (partition) to multiple files (partitions)

```bash
python -m shmr -i <file_path> partitions.coalesce --outfile <output_files> --num_partitions=128
```

2. Parallel applying a mapping function

```bash
ls <input_files> | xargs -n 1 -I {} -P <n_threads> python -m shmr \
    -i {} partition.map --fn <func> --outfile <output_file>
```

If you provide the `-v`, it will show the progression bar telling you how long it will take to process one partition.
