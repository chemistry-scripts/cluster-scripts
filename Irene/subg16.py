#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Entry point for submission of Gaussian16 calculations on Irene

Last Update 2022-01-14 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import argparse
import logging
import os
import shlex
import sys

from computations_setup import Computation


def main():
    """
    Set up the computation and submit it to the job scheduler.

    Precedence of parameters for submission:
        - Command line parameters
        - Gaussian script
        - Default parameters

    Structure of program:
    - Define runvalues:
        - Fill Defaults
        - Parse file and replace appropriately
        - Get command-line parameters and replace appropriately
    - Compute missing values
        - Memory
        - Number of nodes if appropriate
        - Convert filenames to printable values
    - Check parameters
        - Existence of files (or non-existence...)
        - Compatibility nproc/nodes/memory
    - Build script file
    - Submit script
    """
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    # Get parameters from command line
    cmdline_args = get_options()

    # Retrieve input file name, create output file name
    input_file_name = cmdline_args["inputfile"]
    script_file_name = input_file_name + ".sh"

    # Check existence of input file
    if not os.path.exists(input_file_name):
        print("=========== !!! WARNING !!! ===========")
        print("The input file was not found.")
        sys.exit()
    # Check that file input.sh does not exist, not to start twice the same job
    if os.path.exists(script_file_name):
        print("=========== !!! WARNING !!! ===========")
        print(" The corresponding .sh file already exists ")
        print("Make sure it is not a mistake, erase it and rerun the script")
        print("Alternatively, you can submit the job directly with:")
        print("ccc_msub {0}".format(shlex.quote(script_file_name)))
        sys.exit()
    # Avoid end of line problems due to conversion between Windows and Unix
    # file endings
    #    os.system("dos2unix {0}".format(shlex.quote(input_file_name)))
    text = open(input_file_name, "r").read().replace("\r\n", "\n")
    open(input_file_name, "w").write(text)

    # Create computation object
    computation = Computation(input_file_name, "g16", cmdline_args)

    # Create run file for gaussian
    computation.create_run_file(script_file_name)

    # Submit the script
    os.system("ccc_msub {0}".format(shlex.quote(script_file_name)))
    print(
        "job {0} submitted with a walltime of {1} hours".format(
            input_file_name, computation.walltime
        )
    )


def get_options():
    """Check command line options and accordingly set computation parameters."""
    parser = argparse.ArgumentParser(
        description=help_description(), epilog=help_epilog()
    )
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.add_argument(
        "-p", "--proc", type=int, help="Number of processors used for the computation"
    )
    parser.add_argument(
        "-n", "--nodes", type=int, help="Number of nodes used for the computation"
    )
    parser.add_argument(
        "-t",
        "--walltime",
        default="24:00:00",
        type=str,
        help="Maximum time allowed for the computation",
    )
    parser.add_argument(
        "-m",
        "--memory",
        type=int,
        help="Amount of memory allowed for the computation, in MB",
    )
    parser.add_argument("inputfile", type=str, nargs=1, help="The input file to submit")

    try:
        args = parser.parse_args()
    except argparse.ArgumentError as error:
        print(str(error))  # Print something like "option -a not recognized"
        sys.exit(2)

    # Get values from parser
    cmdline_args = dict.fromkeys(["inputfile", "walltime", "memory", "cores", "nodes"])
    cmdline_args["inputfile"] = os.path.basename(args.inputfile[0])
    cmdline_args["walltime"] = args.walltime
    if args.proc:
        cmdline_args["cores"] = args.proc
    if args.nodes:
        cmdline_args["nodes"] = args.nodes
    if args.memory:
        cmdline_args["memory"] = args.memory

    return cmdline_args


def help_description():
    """Return description of program for help message."""
    return """
Setup and submit a job to the SLURM queueing system on Irene cluster.
The job script name should end with .gjf or .com.
"""


def help_epilog():
    """Return additional help message."""
    return """
Defaults values:
  Default memory:          160GB
  Default cores:           48
  Default walltime:        24:00:00

Values for number of cores used and memory to use are read in the input file,
but are overridden if command line arguments are provided.
"""


if __name__ == "__main__":
    main()
