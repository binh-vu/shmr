import csv
from io import StringIO, BytesIO
from typing import Any
from cityhash import CityHash64


def csv_loads(line: bytes):
    return next(csv.reader(StringIO(line.decode())))


def csv_dumps(row: list) -> bytes:
    io = StringIO(newline="")
    writer = csv.writer(io)
    writer.writerow(row)
    io.seek(0)
    return io.read().rstrip().encode()


def str_dumps(x: Any):
    return str(x).encode()


def str_loads(x: bytes):
    # remove trailing newline characters
    return x.decode().rstrip('\n\r')


def str2hashnumber(x: str):
    return CityHash64(x)


def identity(x):
    return x


def getitem_0(x):
    return x[0]


def getitem_1(x):
    return x[1]


def getitem_2(x):
    return x[2]