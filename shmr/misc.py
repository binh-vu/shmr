import bz2
import gzip
import importlib


def get_open_fn(infile: str):
    """Get the correct open function for the input file based on its extension. Supported bzip2, gz

    Args:
        infile (str):  the file we wish to open

    Returns:
        Callable: the open function that can use to open the file
    """
    if infile.endswith(".bz2"):
        return bz2.open
    elif infile.endswith(".gz"):
        return gzip.open
    else:
        return open


def create_filepath_template(intemp: str, output_1partition: bool):
    """Process the user input's template to a python string that allows us to pass variables' value
    to get the correct file name.

    There are three variables:
        1. `{auto}`: an auto-incremental ID of the new partition
        2. `{stem}`: the stem of current processing partition.
        3. `{}` or `*`: will be `{auto}` if we are generating multiple partitions, otherwise `{stem}`

    Args:
        intemp (str): user input's template
        output_1partition (bool): true if we are generate one partition

    Returns:
        str: the template that we can use the python string's format function to pass the variables' value
    """
    if output_1partition:
        default = "{stem}"
    else:
        default = "{auto:05d}"

    intemp = intemp.replace("*", default)
    intemp= intemp.replace("{}", default)
    intemp = intemp.replace("{auto}", "{auto:05d}")
    return intemp


def get_readable_lines_per_file(lines_per_file: int):
    """Get the human readable for number of lines per files

    Parameters
    ----------
    lines_per_file : int
        number of lines per file
    """
    lpf = lines_per_file
    units = ["", "k", "m", "b"]
    i = 0
    while lpf >= 1000:
        lpf = lpf / 1000
        i += 1

    assert lpf == int(lpf), "Please choose a better number"
    return f"{int(lpf)}{units[i]}"


def get_func_by_name(fn_name):
    """Automatically import function by its name

    Args:
        fn_name (str): import path of a function

    Returns:
        Callable: a python function
    """
    module, fn = fn_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module)
    except ModuleNotFoundError:
        module, clazz = module.rsplit(".", 1)
        module = importlib.import_module(module)
        module = getattr(module, clazz)

    return getattr(module, fn)


class fake_tqdm:

    def __init__(self):
        pass

    def update(self, index: int):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # return False otherwise Python will suppress the exception
        return False
