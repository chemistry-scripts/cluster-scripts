#!/usr/bin/env python3

"""
Entry point for submission of Orca 6 calculations on Adastra.

Last Update 2026-09-07 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import argparse
import logging
import os
import shlex
import sys

from computations_setup import Computation


def get_options():
    """Parse and return the command line options for the computation."""
    parser = argparse.ArgumentParser(
        description="Setup and submit an ORCA job to the SLURM queueing system on "
        "Adastra.",
        epilog=(
            "Defaults values:\n"
            "  Default memory:          90GB\n"
            "  Default cores:           24\n"
            "  Default walltime:        24:00:00\n\n"
            "Values for number of cores and memory are read from the input file, "
            "but are overridden if command line arguments are provided."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-p", "--proc", type=int, help="Number of processors used for the computation"
    )
    parser.add_argument(
        "-t",
        "--walltime",
        default="24:00:00",
        help="Maximum time allowed for the computation",
    )
    parser.add_argument(
        "-m",
        "--memory",
        type=int,
        help="Amount of memory allowed for the computation, in MB",
    )
    parser.add_argument("inputfile", nargs=1, help="The input file to submit")

    args = parser.parse_args()

    return {
        "inputfile": os.path.basename(args.inputfile[0]),
        "walltime": args.walltime,
        "cores": args.proc,
        "memory": args.memory,
    }


def main():
    """Set up the computation and submit it to the job scheduler."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s :: %(levelname)s :: %(message)s"
    )

    args = get_options()
    input_file = args["inputfile"]
    script_file = os.path.splitext(input_file)[0] + ".sh"

    # Refuse to run if the input file is missing.
    if not os.path.exists(input_file):
        print("=========== !!! WARNING !!! ===========")
        print("The input file was not found.")
        sys.exit(1)

    # Refuse to run twice: a leftover .sh file would resubmit the same job.
    if os.path.exists(script_file):
        print("=========== !!! WARNING !!! ===========")
        print(" The corresponding .sh file already exists ")
        print("Make sure it is not a mistake, erase it and rerun the script")
        print("Alternatively, you can submit the job directly with:")
        print(f"ccc_msub {shlex.quote(script_file)}")
        sys.exit(1)

    # Normalize Windows line endings in the input file.
    with open(input_file, "r+") as f:
        text = f.read().replace("\r\n", "\n")
        f.seek(0)
        f.write(text)
        f.truncate()

    computation = Computation(input_file, "orca6", args)
    computation.create_run_file(script_file)

    os.system(f"sbatch {shlex.quote(script_file)}")
    print(f"job {input_file} submitted with a walltime of {computation.walltime} hours")


if __name__ == "__main__":
    main()
