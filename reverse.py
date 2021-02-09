import argparse
import concurrent.futures
import mmap
import multiprocessing
import multiprocessing.pool
import pathlib
import resource
import sys
import typing

PARALLELISM = 8
BUFFER = 1024 * 1024


def reverse_buffer(file: pathlib.Path) -> None:
    """
    Reads the file in `BUFFER` chunks, reverse each chunk using string
    slicing, and appends the reversed slice to the output until there is no
    more data to read.
    """
    with open(file, "r") as in_file:
        with open(f"{file}.reversed", "w") as out_file:
            in_file.seek(0, 2)
            pointer_end = in_file.tell()
            pointer_start = pointer_end - min(pointer_end, BUFFER)
            in_file.seek(pointer_start)
            data = in_file.read(BUFFER).strip()

            while len(data) > 0:
                out_file.write(data[::-1])
                pointer_start = pointer_end - min(pointer_end, BUFFER)
                pointer_end = pointer_start
                in_file.seek(pointer_start)
                data = in_file.read(pointer_end - pointer_start)
                pointer_end = in_file.tell()

            out_file.write("\n")


def reverse_naive(file: pathlib.Path) -> None:
    """
    Naively reads the entire file to memory and use string slicing to
    revert it. This is the simplest method, but it reads the entire file to
    memory which is not ideal.
    """
    with open(file, "r") as in_file:
        with open(f"{file}.reversed", "w") as out_file:
            out_file.write(in_file.read()[:-1][::-1])
            out_file.write("\n")


def reverse_mmap(file):
    """
    Creates two memory maps (one for each input/output) and copies input
    bytes to output from last to first.
    """
    with open(file, "r+b") as in_file:
        in_file.seek(0, 2)
        size = in_file.tell()

        with open(f"{file}.reversed", "w+b") as out_file:
            out_file.write(b"\0")
            out_file.flush()

            with mmap.mmap(in_file.fileno(), 0, access=mmap.ACCESS_WRITE) as in_map:
                with mmap.mmap(out_file.fileno(), 0) as out_map:
                    out_map.resize(size)

                    for i in range(1, size):
                        out_map.write_byte(in_map[size - 1 - i])

                    out_map.write(b"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="reverse.py")
    parser.add_argument("method", nargs="?", default="buffer")
    parser.add_argument("pool", nargs="?", default="thread")
    args = parser.parse_args()
    function = {
        "naive": reverse_naive,
        "buffer": reverse_buffer,
        "mmap": reverse_mmap,
    }[args.method]
    paths = pathlib.Path("./data").glob("*.data")

    pool: typing.Union[multiprocessing.pool.Pool, concurrent.futures.ThreadPoolExecutor]

    if args.pool == "process":
        pool = multiprocessing.Pool(PARALLELISM)
    elif args.pool == "thread":
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=PARALLELISM)
    else:
        print("Invalid pool type")
        sys.exit(1)

    with pool as p:
        pool.map(function, paths)

    # mem_self = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    # mem_children = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024.0

    exit(0)