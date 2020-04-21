import bz2
import gzip
import importlib


def get_open_fn(infile: str):
    """Get the correct open function for the input file based on its extension. Supported bzip2, gz

    Parameters
    ----------
    infile : str
        the file we wish to open

    Returns
    -------
    Callable
        the open function that can use to open the file

    Raises
    ------
    ValueError
        when encounter unknown extension
    """
    if infile.endswith(".bz2"):
        return bz2.open
    elif infile.endswith(".gz"):
        return gzip.open
    else:
        return open


def get_filepath_template(file_template: str):
    """
    Parameters
    ----------
    file_template
        the pattern for the output files, the `*` or `{}` placeholder are reserved for placing the chunk name.
        if none of the placeholders were specified, the chunk name will be placed before the extensions
    Returns
    -------
    """
    if file_template.find("*") != -1:
        parts = file_template.rsplit("*", 1)
    elif file_template.find("{}") != -1:
        parts = file_template.split("{}")
    else:
        parts = file_template.rsplit(".", 1)
        if len(parts) == 0:
            parts.append("")
        parts[1] = "." + parts[1]

    if len(parts) != 2:
        raise ValueError("Invalid file template")

    return f"{parts[0]}%05d{parts[1]}"


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
