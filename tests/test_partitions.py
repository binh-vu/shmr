import glob
import shutil
import tempfile
from typing import List, Dict, Tuple, Callable, Any, Optional

from shmr.main import main


def test_coalesce(people, resource_dir):
    tmp_dir = resource_dir / "tmp"
    if tmp_dir.exists():
        shutil.rmtree(str(tmp_dir))

    tmp_dir.mkdir(parents=True)
    main(["-i", people[0], "--skip_nrows", "1",
          "-d", "shmr.csv_loads", "-s", "shmr.str_dumps",
          "partitions.coalesce",
          "--outfile", str(tmp_dir / "*.csv"),
          "--records_per_partition", "50"])

    assert len(glob.glob(str(tmp_dir / "*.csv"))) == 2
    if tmp_dir.exists():
        shutil.rmtree(str(tmp_dir))
