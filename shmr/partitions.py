import contextlib
import glob
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Callable, Any

import orjson
from tqdm.auto import tqdm

from shmr.misc import get_filepath_template, get_open_fn
from shmr.partition import Partition
from shmr.partition_writer import PartitionWriter


class ListPartition:

    def __init__(self, infile: str, skip_nrows: int, deser_fn: Callable[[bytes], Any], ser_fn: Callable[[Any], bytes]):
        partitions = []
        for file_path in sorted(glob.glob(infile)):
            partitions.append(Partition(file_path, deser_fn, ser_fn, skip_nrows))

        if len(partitions) == 0:
            raise ValueError(f"No partition matches the pattern: {infile}")
        self.partitions: List[Partition] = partitions
        self.size = self._size()

    def _size(self) -> Optional[int]:
        size = 0
        for part in self.partitions:
            if part.n_records is None:
                return None
            size += part.n_records
        return size

    def coalesce(self, outfile: str, records_per_partition: Optional[int] = None,
                 num_partitions: Optional[int] = None, verbose: bool = True):
        """The current partitions are coalesce into `num_partitions` partitions. If `num_partitions` is not specified, it will be figure out automatically
        from `records_per_partition`.
        
        Args:
            outfile (str): path template of output partitions
            records_per_partition (Optional[int], optional): number of records per partition. Defaults to None.
            num_partitions (Optional[int], optional): number of partitions. Defaults to None.
            verbose (bool, optional): log execution progress. Defaults to True.
        
        Raises:
            ValueError: if the output directory does not exist
        """
        outfile = get_filepath_template(outfile)
        if not Path(outfile).parent.exists():
            raise ValueError(f"Output directory does not exist: {Path(outfile).parent}")

        if records_per_partition is None:
            assert num_partitions is not None
            # TODO: cal the records_per_partition based on num_partitions

        part_counter = 0
        start = 0

        writer = None
        try:
            writer = PartitionWriter(outfile % part_counter).open()
            for inpart in self.partitions:
                with inpart._open() as f:
                    for i, line in (tqdm(enumerate(f), initial=start, total=self.size) if verbose else enumerate(f)):
                        if (i + 1) % records_per_partition == 0:
                            writer.close()
                            part_counter += 1
                            writer = PartitionWriter(outfile % part_counter).open()
                        writer.write(line)
        finally:
            if writer is not None:
                writer.close()

    def concat(self, outfile: str, verbose: bool = True):
        """Concatenate fdsfdsfsd

        Args:
            outfile: flksdjfldskj
            verbose: fdksljfldskj

        Returns:
            fdsfsd
        """
        with PartitionWriter(outfile).open() as g:
            start = 0
            for inpart in self.partitions:
                with inpart._open() as f:
                    for line in (tqdm(f, initial=start, total=self.size) if verbose else f):
                        g.write(line)
