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
- [Installation](#installation)
- [Features and examples](#features)

## Installation

From PyPi: `pip install shmr`

## Features

This library is designed to work with [xargs](https://en.wikipedia.org/wiki/Xargs) or [parallel](https://www.gnu.org/software/parallel/) for paralleling processing large data as **simple** as possible. Its main goal is to reduce the time spending writing code with respect to reasonable computing speed up by doing parallelization (i.e., not trying to be as fast as possible, but still faster than sequential algorithms). It is more suitable to research environment than production environment as existing parallel computing frameworks.

Its API is highly influent by Spark API.

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
