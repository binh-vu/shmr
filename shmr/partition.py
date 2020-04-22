import contextlib
import os
from typing import Optional, Callable, Any

from tqdm import tqdm

from shmr.misc import get_open_fn, get_func_by_name, create_filepath_template
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
        self.stem = os.path.splitext(os.path.basename(self.path))[0]

    def _open(self):
        f = get_open_fn(self.path)(self.path, "rb")
        for i in range(self.skip_nrows):
            next(f)
        return f

    def map(self, fn: str, outfile: str, verbose: bool = True):
        """Apply a map function on every record of this partition
        
        Args:
            fn (str): an import path of the map function, which should has this signature: (record: Any) -> Any
            outfile (str): output file of the new partition, `*` or `{stem}` in the path are placeholders, which will be replaced by the stem (i.e., file name without extension) of the current partition
            verbose (bool, optional): show the execution progress bar. Defaults to True.
        """
        fn = get_func_by_name(fn)
        outfile = create_filepath_template(outfile, True).format(auto=0, stem=self.stem)

        with self._open() as f, PartitionWriter(outfile) as g:
            for record in tqdm(f, total=self.n_records) if verbose else f:
                record = self.deser_fn(record)
                record = fn(record)
                record = self.ser_fn(record)
                g.write(record)
                g.write_new_line()

    def count(self, outfile: Optional[str] = None, verbose: bool = True) -> int:
        """Count

        Args:
            outfile (Optional[str], optional): output file to write to the value to if it is not None. if outfile is stdout we will print to stdout
            verbose (bool, optional): [description]. Defaults to True.

        Returns:
            the number of records
        """
        if self.n_records is None:
            n_records = 0
            with self._open() as f:
                for _ in tqdm(f) if verbose else f:
                    n_records += 1
            PartitionMetadata(self.path).write({"n_records": n_records})
            self.n_records = n_records

        if outfile is not None:
            if outfile == "stdout":
                print(self.n_records)
            else:
                outfile = create_filepath_template(outfile, True).format(auto=0, stem=self.stem)
                with open(outfile, "w") as f:
                    f.write(str(self.n_records))

        return self.n_records

    def filter(self, fn: str, outfile: str, delete_on_empty: bool = False, verbose: bool = True):
        """Filter function
        
        Args:
            fn (str): [description]
            outfile (str): [description]
            delete_on_empty (bool, optional): delete the partition if there is no record. Defaults to False
            verbose (bool, optional): [description]. Defaults to True.
        """
        fn = get_func_by_name(fn)
        outfile = create_filepath_template(outfile, True).format(auto=0, stem=self.stem)

        with self._open() as f, PartitionWriter(outfile, on_close_delete_if_empty=delete_on_empty) as g:
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
        outfile = create_filepath_template(outfile, False)
        fn = get_func_by_name(fn)

        with contextlib.ExitStack() as stack, self._open() as f:
            writers = [
                stack.enter_context(PartitionWriter(outfile.format(auto=i, stem=self.stem)))
                for i in range(num_partitions)
            ]
            for line in tqdm(f, total=self.n_records) if verbose else f:
                bucket_no = fn(self.deser_fn(line))
                partno = bucket_no % num_partitions
                writers[partno].write(line)

    def reduce(self, fn: str, outfile: str, init_val: Any = None, verbose: bool = True):
        """Reduce
        
        Args:
            fn (str): [description]
            outfile (str): [description]
            init_val (Any): [description]
            verbose (bool, optional): [description]. Defaults to True.
        """
        fn = get_func_by_name(fn)
        outfile = create_filepath_template(outfile, True).format(auto=0, stem=self.stem)
        if init_val is not None:
            accum = init_val
        else:
            accum = None

        with self._open() as f, PartitionWriter(outfile) as g:
            if accum is None:
                try:
                    record = self.deser_fn(next(f))
                    accum = fn(record)
                except StopIteration:
                    pass

            for line in tqdm(f, total=self.n_records - 1 if self.n_records is not None else None) if verbose else f:
                record = self.deser_fn(line)
                accum = fn(record, accum)

            g.write(self.ser_fn(accum))
            g.write_new_line()

    def head(self, n_rows: int):
        """Print first n rows

        Args:
            n_rows (int): number of rows to print
        """
        with self._open() as f:
            try:
                for i in range(n_rows):
                    print(next(f))
            except StopIteration:
                return
