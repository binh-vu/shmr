import contextlib
import pickle
from typing import Optional, Callable, Any, Union

import orjson
from tqdm import tqdm

from shmr.misc import get_open_fn, get_func_by_name, get_filepath_template
from shmr.partition_writer import PartitionWriter, PartitionMetadata


class Partition:

    def __init__(self,
                 path: str,
                 deser_fn: Callable[[bytes], Any],
                 ser_fn: Callable[[Any], bytes],
                 skip_nrows: int = 0):
        metadata = PartitionMetadata(path).read()

        self.path = path
        self.deser_fn = deser_fn
        self.ser_fn = ser_fn
        self.skip_nrows = skip_nrows

        self.n_records: Optional[int] = metadata.get("n_records", None)

    def _open(self):
        f = get_open_fn(self.path)(self.path, "rb")
        for i in range(self.skip_nrows):
            next(f)
        return f

    def map(self, fn: str, outfile: str, verbose: bool = True):
        """Map
        
        Args:
            fn (str): [description]
            outfile (str): [description]
            verbose (bool, optional): [description]. Defaults to True.
        """
        fn = get_func_by_name(fn)

        with self._open() as f, PartitionWriter(outfile) as g:
            for record in tqdm(f, total=self.n_records) if verbose else f:
                record = self.deser_fn(record)
                record = fn(record)
                record = self.ser_fn(record)
                g.write(record)
                g.write_new_line()

    def count(self, outfile: str, verbose: bool = True):
        """Count
        
        Args:
            verbose (bool, optional): [description]. Defaults to True.
            outfile (str): output file
        """
        if self.n_records is None:
            n_records = 0
            with self._open() as f:
                for _ in tqdm(f) if verbose else f:
                    n_records += 1
            PartitionMetadata(self.path).write({"n_records": n_records})
            self.n_records = n_records

        with open(outfile, "w") as f:
            f.write(str(self.n_records))

    def filter(self, fn: str, outfile: str, verbose: bool = True):
        """Filter function
        
        Args:
            fn (str): [description]
            outfile (str): [description]
            verbose (bool, optional): [description]. Defaults to True.
        """
        fn = get_func_by_name(fn)
        with self._open() as f, PartitionWriter(outfile) as g:
            for line in tqdm(f, total=self.n_records) if verbose else f:
                if fn(self.deser_fn(line)):
                    g.write(line)

    def apply(self, fn: str, verbose: bool = True):
        """Apply
        
        Args:
            fn (str): function
            verbose (bool, optional): show execution progress. Defaults to True.
        """
        fn = get_func_by_name(fn)
        with self._open() as f:
            for line in tqdm(f, total=self.n_records) if verbose else f:
                fn(self.deser_fn(line))

    def group_by(self, fn: str, outfile: str, num_partitions: int, verbose: bool = True):
        """Group by
        
        Args:
            fn (str): function
            outfile (str): outfile
            num_partitions (int): [description]
            verbose (bool, optional): [description]. Defaults to True.
        """
        outfile = get_filepath_template(outfile)
        fn = get_func_by_name(fn)

        with contextlib.ExitStack() as stack, self._open() as f:
            writers = [
                stack.enter_context(PartitionWriter(outfile % i))
                for i in range(num_partitions)
            ]
            for line in tqdm(f) if verbose else f:
                bucket_no = fn(self.deser_fn(line))
                partno = bucket_no % num_partitions
                writers[partno].write(line)

    def reduce(self, fn: str, outfile: str, init_val: Any, verbose: bool = True):
        """Reduce
        
        Args:
            fn (str): [description]
            outfile (str): [description]
            init_val (Any): [description]
            verbose (bool, optional): [description]. Defaults to True.
        """
        fn = get_func_by_name(fn)
        with self._open() as f, PartitionWriter(outfile) as g:
            try:
                record = self.deser_fn(next(f))
                accum = fn(init_val, record)
            except StopIteration:
                accum = init_val

            for line in tqdm(f, total=self.n_records - 1) if verbose else f:
                record = self.deser_fn(line)
                accum = fn(accum, record)

            g.write(self.ser_fn(accum))
            g.write_new_line()
