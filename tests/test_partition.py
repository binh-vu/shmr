import glob
import shutil
import tempfile
from typing import List, Dict, Tuple, Callable, Any, Optional
from uuid import uuid4

from shmr import csv_loads
from shmr.main import main


class PeopleFunc:
    @staticmethod
    def get_age(row):
        return int(row[3])

    @staticmethod
    def sum_age(row, accum=0):
        return PeopleFunc.get_age(row) + accum


def test_count(people):
    with tempfile.NamedTemporaryFile() as temp:
        main(["-i", people[0], "--skip_nrows", "1",
              "-d", "shmr.csv_loads", "-s",
              "shmr.csv_dumps",
              "partition.count", "--outfile", temp.name])

        temp.seek(0)
        assert temp.read() == b'100'


def test_map(people):
    with tempfile.NamedTemporaryFile() as temp:
        main(["-i", people[0], "--skip_nrows", "1",
              "-d", "shmr.csv_loads", "-s",
              "shmr.str_dumps",
              "partition.map",
              "--fn", "tests.test_partition.PeopleFunc.get_age",
              "--outfile", temp.name])

        temp.seek(0)
        assert sum(int(line.decode()) for line in temp) == 5047


def test_reduce(people):
    with tempfile.NamedTemporaryFile() as temp:
        main(["-i", people[0], "--skip_nrows", "1",
              "-d", "shmr.csv_loads", "-s",
              "shmr.str_dumps",
              "partition.reduce",
              "--fn", "tests.test_partition.PeopleFunc.sum_age",
              "--outfile", temp.name])

        temp.seek(0)
        assert int(temp.read().decode()) == 5047


def test_group_by(people, resource_dir):
    tmp_dir = resource_dir / str(uuid4())
    tmp_dir.mkdir(exist_ok=True)
    main(["-i", people[0], "--skip_nrows", "1",
          "-d", "shmr.csv_loads", "-s",
          "shmr.str_dumps",
          "partition.group_by",
          "--fn", "tests.test_partition.PeopleFunc.get_age",
          "--num_partitions", "5",
          "--outfile", str(tmp_dir / "*.csv")])

    assert len(glob.glob(str(tmp_dir / "*.csv"))) == 5
    for i, file in enumerate(sorted(glob.glob(str(tmp_dir / "*.csv")))):
        with open(file, "rb") as f:
            for line in f:
                assert (PeopleFunc.get_age(csv_loads(line)) - i) % 5 == 0

    if tmp_dir.exists():
        shutil.rmtree(str(tmp_dir))