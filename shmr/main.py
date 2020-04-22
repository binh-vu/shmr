import inspect
from argparse import ArgumentParser
from time import time
from typing import Set, Optional, Any

import orjson
from docstring_parser import parse as docstring_parse, Style
from fastnumbers import isfloat

from shmr.misc import get_func_by_name
from shmr.partition import Partition
from shmr.partitions import ListPartition
from shmr.version import __version__

CLASSES = {
    "partition": Partition,
    "partitions": ListPartition,
}
FUNCTIONS = {}


def parse_argval(val: str):
    if val.isdigit():
        return int(val)
    if isfloat(val):
        return float(val)
    if val == "set()":
        return set()

    try:
        return orjson.loads(val)
    except ValueError:
        return val


def parse_bool(val: str):
    if val not in {"false", "true", "True", "False"}:
        raise ValueError(f"Invalid boolean value: {val}")
    return val.lower() == 'true'


def build_subparser(subparsers, program, fn, ignore_params: Set[str]):
    global FUNCTIONS

    FUNCTIONS[program] = set()
    signature = inspect.signature(fn)
    doc = docstring_parse(inspect.getdoc(fn), style=Style.google)
    doc_params = {
        p.arg_name: p
        for p in doc.params
    }

    parser = subparsers.add_parser(program, description=doc.short_description, help=doc.short_description)
    it = iter(signature.parameters.values())
    assert next(it).name == 'self'
    for param in it:
        FUNCTIONS[program].add(param.name)
        if param.name in ignore_params:
            continue

        kwargs = {"help": f"({doc_params[param.name].type_name}) {doc_params[param.name].description}"}
        if param.default is param.empty:
            kwargs['required'] = True
        else:
            kwargs['default'] = param.default

        if param.annotation is param.empty:
            pass
        elif param.annotation in (str, int, float):
            kwargs['type'] = param.annotation
        elif param.annotation is bool:
            if param.default is False:
                # special case to make providing boolean values easier
                kwargs['required'] = False
                kwargs['action'] = 'store_true'
            else:
                # if this is not default to be False, then users has to provide the value explicitly
                kwargs['type'] = 'parse_bool'
        elif param.annotation in (Optional[str], Optional[int], Optional[float]):
            param_type = param.annotation.__args__[0]
            assert param_type in (str, int, float)
            kwargs['type'] = param_type
            kwargs['required'] = False
        elif param.annotation is Any:
            kwargs['type'] = parse_argval
        else:
            raise NotImplementedError()

        parser.add_argument(f"--{param.name}", **kwargs)


def build_parser():
    global CLASSES

    parser = ArgumentParser(prog=f"sh map-reduce ({__version__})")
    parser.add_argument("-v", "--verbose", action='count', default=0, help="")
    parser.add_argument("-i", "--infile", required=True,
                        help="the path to one partition or list of partitions depend on the sub-program")
    parser.add_argument("--skip_nrows", type=int, default=0, help="Skip first n rows of each partition")
    parser.add_argument("-d", "--deser_fn", type=str, default='orjson.loads',
                        help="Deserialization function. Default is `orjson.loads`")
    parser.add_argument("-s", "--ser_fn", type=str, default='orjson.dumps',
                        help="Serialization function. Default is `orjson.dumps`")

    subparsers = parser.add_subparsers(help="functions", dest='command')

    for cname, c in CLASSES.items():
        funcs = inspect.getmembers(c, predicate=inspect.isfunction)
        for fname, func in funcs:
            if fname.startswith("_"):
                continue

            build_subparser(subparsers, f"{cname}.{fname}", func, {"verbose"})

    return parser


def main(args):
    # build parser and parse arguments
    start = time()
    parser = build_parser()
    args = parser.parse_args(args)
    end = time()

    args = vars(args)
    command = args.pop("command")
    verbose = args.pop("verbose")
    infile = args.pop("infile")
    skip_nrows = args.pop("skip_nrows")
    deser_fn = get_func_by_name(args.pop("deser_fn"))
    ser_fn = get_func_by_name(args.pop("ser_fn"))

    if verbose > 1:
        print(f">>> run sh map-reduce version {__version__}")
        print(f">>> build parser and parsing arguments takes {end - start:.03f}s")

    classname, method_name = command.rsplit(".", 1)
    instance = CLASSES[classname](infile, deser_fn, ser_fn, skip_nrows)

    fn = getattr(instance, method_name)

    if 'verbose' in FUNCTIONS[command]:
        args['verbose'] = verbose > 0

    if verbose > 1:
        print(f">>> execute functions with arguments:\n%s" % orjson.dumps(args))

    fn(**args)

    if verbose > 1:
        print(f">>> finish! execution time: {time() - start:.03f}s")
