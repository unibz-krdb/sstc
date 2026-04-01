import argparse
import sys
from pathlib import Path

from .generator import Generator
from .transducer_context import TransducerContext


def main():
    parser = argparse.ArgumentParser(description="Semantic SQL Transducer Compiler")
    parser.add_argument("universal", help="Path to universal schema JSON")
    parser.add_argument("source", help="Path to source RA file")
    parser.add_argument("target", help="Path to target RA file")
    parser.add_argument("--output", "-o", help="Output SQL file (default: stdout)")
    args = parser.parse_args()

    ctx = TransducerContext.from_files(args.universal, args.source, args.target)
    sql = Generator(ctx).compile()

    if args.output:
        Path(args.output).write_text(sql)
    else:
        sys.stdout.write(sql)


if __name__ == "__main__":
    main()
