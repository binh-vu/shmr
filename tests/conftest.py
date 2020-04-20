import glob
import os
from pathlib import Path
from typing import List, Dict, Tuple, Callable, Any, Optional

import pytest


@pytest.fixture(scope="module")
def resource_dir() -> Path:
    return Path(os.path.abspath(__file__)).parent / "resources"


@pytest.fixture(scope="module")
def people(resource_dir: Path):
    return sorted(glob.glob(str(resource_dir / "people.*")))
