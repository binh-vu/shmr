import csv
from io import StringIO, BytesIO
from typing import Any


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