import os
from pathlib import Path

import orjson

from shmr.misc import get_open_fn


class PartitionWriter:
    def __init__(self, outfile: str, on_close_delete_if_empty: bool = False, auto_mkdir: bool = False):
        if not Path(outfile).parent.exists():
            if auto_mkdir:
                Path(outfile).parent.mkdir(parents=True)
            else:
                raise ValueError(f"Output directory does not exist: {Path(outfile).parent}")

        self.outfile = outfile
        self.n_records = 0
        self.writer = None
        self.on_close_delete_if_empty = on_close_delete_if_empty

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self.writer is not None:
            raise ValueError("Cannot open the writer twice")

        self.n_records = 0
        self.writer = get_open_fn(self.outfile)(self.outfile, "wb")
        return self

    def write(self, record: bytes):
        self.writer.write(record)
        self.n_records += 1

    def write_new_line(self):
        self.writer.write(b"\n")

    def close(self):
        if self.writer is not None:
            self.writer.close()
            self.writer = None
            PartitionMetadata(self.outfile).write({"n_records": self.n_records})

        if self.on_close_delete_if_empty:
            self.delete_if_empty()

    def delete_if_empty(self):
        if self.writer is not None:
            raise Exception("You need to close the file first")
        if self.n_records == 0:
            self.delete()

    def delete(self):
        if os.path.exists(self.outfile):
            os.remove(self.outfile)
        PartitionMetadata(self.outfile).delete()


class PartitionMetadata:
    def __init__(self, partition_path: str):
        try:
            stempath, extension = partition_path.rsplit(".", 1)
        except ValueError:
            stempath = partition_path
        self.outfile = stempath + ".meta"

    def read(self):
        if not os.path.exists(self.outfile):
            return {}

        with open(self.outfile, "rb") as f:
            return orjson.loads(f.read())

    def write(self, metadata: dict):
        with open(self.outfile, "wb") as f:
            f.write(orjson.dumps(metadata, option=orjson.OPT_INDENT_2))

    def delete(self):
        if os.path.exists(self.outfile):
            os.remove(self.outfile)