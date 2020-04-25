import glob
from math import ceil
from pathlib import Path
from typing import Optional, List, Callable, Any

from tqdm.auto import tqdm

from shmr.misc import create_filepath_template, get_func_by_name, fake_tqdm
from shmr.partition import Partition
from shmr.partition_writer import PartitionWriter


class ListPartition:

    def __init__(self, infile: str, deser_fn: Callable[[bytes], Any], ser_fn: Callable[[Any], bytes], skip_nrows: int):
        partitions = []
        for file_path in sorted(glob.glob(infile)):
            partitions.append(Partition(file_path, deser_fn, ser_fn, skip_nrows))

        if len(partitions) == 0:
            raise ValueError(f"No partition matches the pattern: {infile}")
        self.partitions: List[Partition] = partitions
        self.ser_fn = ser_fn
        self.size = self._size()

    def _size(self) -> Optional[int]:
        size = 0
        for part in self.partitions:
            if part.n_records is None:
                return None
            size += part.n_records
        return size

    def count(self, outfile: Optional[str] = None, auto_mkdir: bool = False, verbose: bool = True):
        """Count the number of records in all partitions

        Args:
            outfile (Optional[str], optional): output file to write to the value to if it is not None. if outfile is stdout we will print to stdout
            auto_mkdir (bool, optional): automatically create directory if the directory of the output file does not exist. Defaults to False
            verbose (bool): print the execution progress
        """
        if self.size is None:
            for inpart in self.partitions:
                inpart.count(verbose=verbose)
            self.size = self._size()

        if outfile is not None:
            if outfile == "stdout":
                print(self.size)
            else:
                if not Path(outfile).parent.exists():
                    if auto_mkdir:
                        Path(outfile).parent.mkdir(parents=True)
                    else:
                        raise ValueError(f"Output directory does not exist: {Path(outfile).parent}")

                with open(outfile, "w") as f:
                    f.write(str(self.size))

        return self.size

    def coalesce(self, outfile: str, records_per_partition: Optional[int] = None,
                 num_partitions: Optional[int] = None, auto_mkdir: bool = False, verbose: bool = True):
        """The current partitions are coalesce into `num_partitions` partitions. If `num_partitions` is not specified, it will be figure out automatically
        from `records_per_partition`.
        
        Args:
            outfile (str): path template of output partitions
            records_per_partition (Optional[int], optional): number of records per partition. Defaults to None.
            num_partitions (Optional[int], optional): number of partitions. Defaults to None.
            auto_mkdir (bool, optional): automatically create directory if the directory of the output file does not exist. Defaults to False
            verbose (bool, optional): log execution progress. Defaults to True.
        
        Raises:
            ValueError: if the output directory does not exist
        """
        outfile = create_filepath_template(outfile, False)
        if records_per_partition is None:
            assert num_partitions is not None
            assert self.size is not None, "Cannot determine the records per partition based on number of partitions because of unknown size of partitions. Consider running partition.count or provide the `records_per_partition` parameter"
            records_per_partition = ceil(self.size / num_partitions)

        writer = None
        part_counter = 0

        with (tqdm(total=self.size) if verbose else fake_tqdm()) as pbar:
            try:
                writer = PartitionWriter(outfile.format(auto=part_counter, stem=""),
                                         on_close_delete_if_empty=True, auto_mkdir=auto_mkdir).open()
                for inpart in self.partitions:
                    with inpart._open() as f:
                        for i, line in enumerate(f):
                            writer.write(line)
                            pbar.update(1)

                            if (i + 1) % records_per_partition == 0:
                                writer.close()
                                part_counter += 1
                                writer = PartitionWriter(outfile.format(auto=part_counter, stem=""),
                                                         on_close_delete_if_empty=True).open()

            finally:
                if writer is not None:
                    writer.close()

    def concat(self, outfile: str, auto_mkdir: bool = False, verbose: bool = True):
        """Concatenate partitions to one file

        Args:
            outfile (str): path to output partition
            auto_mkdir (bool, optional): automatically create directory if the directory of the output file does not exist. Defaults to False
            verbose (bool): log execution progress. Defaults to True

        Returns:
            ValueError: if the output directory does not exist
        """
        outfile = create_filepath_template(outfile, False).format(auto=0, stem="")
        with PartitionWriter(outfile, auto_mkdir=auto_mkdir) as g, (tqdm(total=self.size) if verbose else fake_tqdm()) as pbar:
            for inpart in self.partitions:
                with inpart._open() as f:
                    for line in f:
                        g.write(line)
                        pbar.update(1)

    def reduce(self, fn: str, outfile: str, init_val: Any = None, auto_mkdir: bool = False, verbose: bool = True):
        """Reduce

        Args:
            fn (str): [description]
            outfile (str): [description]
            init_val (Any): [description]
            auto_mkdir (bool, optional): automatically create directory if the directory of the output file does not exist. Defaults to False
            verbose (bool, optional): [description]. Defaults to True.
        """
        fn = get_func_by_name(fn)
        if init_val is not None:
            accum = init_val
        else:
            accum = None

        outfile = create_filepath_template(outfile, False).format(auto=0, stem="")
        with PartitionWriter(outfile, auto_mkdir=auto_mkdir) as g, (tqdm(total=self.size) if verbose else fake_tqdm()) as pbar:
            for inpart in self.partitions:
                with inpart._open() as f:
                    if accum is None:
                        try:
                            record = inpart.deser_fn(next(f))
                            accum = fn(record)
                            if verbose:
                                pbar.update(1)
                        except StopIteration:
                            pass

                    for line in f:
                        record = inpart.deser_fn(line)
                        accum = fn(record, accum)
                        pbar.update(1)

            g.write(self.ser_fn(accum))
            g.write_new_line()

    def head(self, n: int):
        """Print first n rows

        Args:
            n (int): number of rows to print
        """
        count = 0
        for inpart in self.partitions:
            if count >= n:
                break

            with inpart._open() as f:
                for line in f:
                    print(line)
                    count += 1

                    if count >= n:
                        break
