import sys
from argparse import ArgumentParser
from typing import List, Dict, Tuple, Callable, Any, Optional
from uuid import uuid4
from collections import  OrderedDict

from shmr.version import __version__
from shmr.main import parse_bool, parse_argval


def build_parser():
    parser = ArgumentParser(prog=f"sh map-reduce ({__version__})")
    subparsers = parser.add_subparsers(dest="command")

    # funcs = OrderedDict([
    #     ("map", [("fn", str)]),
    #     ("flat_map", [("fn", str)]),
    #     ("filter", [("fn", str)]),
    #     ("reduce", [("fn", str), ("init_val", (Any, None))]),
    # ])
    funcs = OrderedDict([
        ("map", ["fn"]),
        ("flat_map", ["fn"]),
        ("filter", ["fn"]),
        ("reduce", ["fn", "init_val"]),
        ("distinct", ["key_fn", "num_partitions"]),
    ])

    empty = uuid4()

    for fname, fargs in funcs.items():
        subparser = subparsers.add_parser(fname)

        subparser.add_argument("--skip_nrows", type=int, default=0, help="Skip first n rows of each partition")
        subparser.add_argument("--auto_mkdir", action='store_true', help="Auto-make output directory")
        subparser.add_argument("-d", "--deser_fn", type=str, default='orjson.loads',
                            help="Deserialization function. Default is `orjson.loads`")
        subparser.add_argument("-s", "--ser_fn", type=str, default='orjson.dumps',
                            help="Serialization function. Default is `orjson.dumps`")
        subparser.add_argument("-v", "--verbose", action='count', default=0, help="")
        subparser.add_argument("-i", "--infiles", required=True,
                            help="input partitions")
        subparser.add_argument("-o", "--outfiles", required=True,
                            help="path template for output partitions")

        for argname in fargs:
            subparser.add_argument(f"--{argname}", default="")
        # for argname, argtype in fargs:
        #     kwargs = {}
        #     if isinstance(argtype, (tuple, list)):
        #         argtype, default = argtype
        #         kwargs['default'] = default
        #         kwargs['required'] = False
        #     else:
        #         default = empty
        #         kwargs['required'] = True
        #
        #     if argtype is bool:
        #         if default is False:
        #             kwargs['required'] = False
        #             kwargs['action'] = 'store_true'
        #         else:
        #             kwargs['type'] = 'parse_bool'
        #     elif argtype in (str, int, float):
        #         kwargs['type'] = argtype
        #     elif argtype is Any:
        #         kwargs['type'] = parse_argval
        #     else:
        #         raise NotImplementedError()
        #
        #     subparser.add_argument(f"--{argname}", **kwargs)

    return parser, funcs


def bash_main(args):
    parser, funcs = build_parser()
    args = parser.parse_args(args)

    output_order = ["skip_nrows", "auto_mkdir", "deser_fn", "ser_fn", "verbose", "infiles", "outfiles"]
    output_args = []

    for name in output_order:
        if name == "verbose":
            output_args.append("-" + ("v" * args.verbose))
        else:
            output_args.append(getattr(args, name))

    for argname in funcs[args.command]:
        output_args.append(getattr(args, argname))

    print(" ".join([str(x) for x in output_args]))


if __name__ == '__main__':
    bash_main(sys.argv[1:])
