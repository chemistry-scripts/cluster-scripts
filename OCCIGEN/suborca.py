#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Submit script for Orca for Computing Clusters.

Script adapted for Cines OCCIGEN cluster
Last Update 2017-10-30 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed and accessible.
"""

import argparse
import sys
import os
import logging
import shlex


def main():
    """
    Set up the computation and submit it to the job scheduler.

    Precedence of parameters for submission:
        - Command line parameters
        - ORCA script
        - Default parameters

    Structure of program:
    - Define runvalues:
        - Fill Defaults
        - Parse file and replace appropriately
        - Get command-line parameters and replace appropriately
    - Compute missing values
        - Memory
        - Number of nodes if appropriate
        - Cluster section (HSW24/BDW28)
        - Convert filenames to printable values
    - Check parameters
        - Existence of files (or non-existence...)
        - Compatibility nproc/nodes/memory
    - Build script file
    - Submit script
    """
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    # Setup runvalues with default settings
    runvalues = default_run_values()

    # Get parameters from command line
    cmdline_args = get_options()

    # Retrieve input file name, create output file name
    input_file_name = cmdline_args['inputfile']
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
        print("sbatch {0}".format(shlex.quote(script_file_name)))
        sys.exit()
    # Avoid end of line problems due to conversion between Windows and Unix
    # file endings
    os.system('dos2unix {0}'.format(shlex.quote(input_file_name)))

    # Get computation parameters from input file
    runvalues = get_values_from_input_file(input_file_name, runvalues)

    # Merge command-line parameters into runvalues
    runvalues = fill_from_commandline(runvalues, cmdline_args)

    # Fill with missing values and consolidate the whole thing
    runvalues = fill_missing_values(runvalues)

    # Create run file for ORCA
    create_run_file(script_file_name, runvalues)
    # Submit the script
    os.system('sbatch {0}'.format(shlex.quote(script_file_name)))
    print("job {0} submitted with a walltime of {1} hours"
          .format(input_file_name, runvalues['walltime']))


def get_options():
    """Check command line options and accordingly set computation parameters."""
    parser = argparse.ArgumentParser(description=help_description(),
                                     epilog=help_epilog())
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.add_argument('-p', '--proc', type=int,
                        help="Number of processors used for the computation")
    parser.add_argument('-n', '--nodes', type=int,
                        help="Number of nodes used for the computation")
    parser.add_argument('-t', '--walltime', default="24:00:00", type=str,
                        help="Maximum time allowed for the computation")
    parser.add_argument('-m', '--memory', type=int,
                        help="Amount of memory allowed for the computation, in MB")
    parser.add_argument('inputfile', type=str, nargs=1,
                        help='The input file to submit')

    try:
        args = parser.parse_args()
    except argparse.ArgumentError as error:
        print(str(error))  # Print something like "option -a not recognized"
        sys.exit(2)

    # Get values from parser
    cmdline_args = dict.fromkeys(['inputfile', 'walltime', 'memory', 'cores', 'nodes'])
    cmdline_args['inputfile'] = os.path.basename(args.inputfile[0])
    cmdline_args['walltime'] = args.walltime
    if args.proc:
        cmdline_args['cores'] = args.proc
    if args.nodes:
        cmdline_args['nodes'] = args.nodes
    if args.memory:
        cmdline_args['memory'] = args.memory

    return cmdline_args


def fill_from_commandline(runvalues, cmdline_args):
    """Merge command line arguments into runvalues."""
    runvalues['inputfile'] = cmdline_args['inputfile']
    if cmdline_args['nodes']:
        runvalues['nodes'] = cmdline_args['nodes']
    if cmdline_args['cores']:
        runvalues['cores'] = cmdline_args['cores']
    if cmdline_args['walltime']:
        runvalues['walltime'] = cmdline_args['walltime']
    if cmdline_args['memory']:
        runvalues['memory'] = cmdline_args['memory']
    return runvalues


def default_run_values():
    """Fill default runvalues."""
    # Setup runvalues
    runvalues = dict.fromkeys(['inputfile', 'outputfile', 'nodes', 'cores', 'walltime', 'memory',
                               'nproc_in_input', 'cluster_section'])
    runvalues['inputfile'] = ''
    runvalues['outputfile'] = ''
    runvalues['nodes'] = 1
    runvalues['cores'] = 24
    runvalues['walltime'] = '24:00:00'
    runvalues['memory'] = 58000  # In MB
    runvalues['nproc_in_input'] = False
    runvalues['nbo'] = False
    runvalues['nbo_basefilename'] = ''
    runvalues['cluster_section'] = 'HSW24'
    return runvalues


def get_values_from_input_file(input_file, runvalues):
    """Get core/memory values from input file, reading the Mem and NProcShared parameters."""
    with open(input_file, 'r') as file:
        # Go through lines and test if they are containing nproc, mem, etc. related
        # directives.
        for line in file.readlines():
            if "nprocs" in line.lower():
                runvalues['nproc_in_input'] = True
                proc_line = line.split()
                runvalues['cores'] = int(proc_line[1])
            if "nbo6" in line.lower() or "npa6" in line.lower():
                runvalues['nbo'] = True
            if "TITLE=" in line:
                # TITLE=FILENAME
                runvalues['nbo_basefilename'] = line.split('=')[1]

    return runvalues


def fill_missing_values(runvalues):
    """Compute and fill all missing values."""
    # Setup cluster_section according to number of cores
    if runvalues['cores'] <= 24:
        runvalues['cluster_section'] = "HSW24"
    elif runvalues['cores'] <= 28:
        runvalues['cluster_section'] = "BDW28"
    elif runvalues['cores'] > 28 and runvalues['nodes'] == 1:
        raise ValueError("Number of cores cannot exceed 28 for one node.")
    elif runvalues['nodes'] > 1:
        raise ValueError("Multiple nodes not supported at the moment.")

    # TODO: manage the multiple nodes case

    # TODO; Better memory checks
    memory = compute_memory(runvalues)
    runvalues['memory'] = memory

    return runvalues


def create_shlexnames(runvalues):
    """Return dictionary containing shell escaped names for all possible files."""
    shlexnames = dict()
    input_basename = os.path.splitext(runvalues['inputfile'])[0]
    shlexnames['inputfile'] = shlex.quote(runvalues['inputfile'])
    shlexnames['basename'] = shlex.quote(input_basename)
    return shlexnames


def compute_memory(runvalues):
    """
    Return ideal memory value for OCCIGEN.

    4GB per core, or memory from input + 4000 MB for overhead.
    Computed to use as close as possible the memory available.
    """
    if runvalues['memory'] is None:
        if runvalues['cores'] <= 24:
            memory = runvalues['cores'] * 4800
        elif runvalues['cores'] == 28:
            memory = 59000

    return memory


def create_run_file(output, runvalues):
    """
    Create .sh file that contains the script to actually run on the server.

    Structure:
    -- SBATCH instructions for the queue manager
    -- setup of ORCA on the nodes
    -- creation of scratch, copy necessary files
    -- Run ORCA
    -- Copy and rename appropriate files back to $HOME
    -- Cleanup scratch

    Instructions adapted from www.cines.fr
    """
    # Setup logging
    logger = logging.getLogger()

    # FROM OCCIGEN Reference:
    # =====> export OMP_NUM_THREADS=24
    # =====> export KMP_AFFINITY=compact,1,0
    # =====> Start ORCA with full path
    # Setup names to use in file
    shlexnames = create_shlexnames(runvalues)
    logger.debug("Runvalues:  %s", runvalues)
    logger.debug("Shlexnames: %s", shlexnames)

    # TODO: multi-nodes
    # On SLURM, memory is defined per node
    # #SBATCH --nodes=2
    # #SBATCH --ntasks=48
    # #SBATCH --ntasks-per-node=24
    # #SBATCH --threads-per-core=1
    out = ['#!/bin/bash\n',
           '#SBATCH -J ' + shlexnames['inputfile'] + '\n',
           '#SBATCH --constraint=' + runvalues['cluster_section'] + '\n'
           '#SBATCH --mail-type=ALL\n',
           '#SBATCH --mail-user=user@server.org\n',
           '#SBATCH --nodes=' + str(runvalues['nodes']) + '\n',
           '#SBATCH --ntasks=' + str(runvalues['cores']) + '\n',
           '#SBATCH --mem=' + str(runvalues['memory']) + '\n',
           '#SBATCH --time=' + runvalues['walltime'] + '\n',
           '#SBATCH --output=' + shlexnames['basename'] + '.slurmout\n',
           '\n']
    out.extend(['# Load modules necessary for ORCA\n',
                'module purge\n',
                'module load intel/18.0\n',
                'module load openmpi/intel/2.0.2\n',
                '\n'])
    if runvalues['nbo']:
        out.extend(['# Setup NBO6\n',
                    'export NBOBIN=$SHAREDHOMEDIR/nbo6/bin\n',
                    'export PATH=$PATH:$NBOBIN\n',
                    '\n'])
    out.extend(['# Setup Scratch\n',
                'export ORCA_TMPDIR=$SCRATCHDIR/orca/$SLURM_JOBID\n',
                'mkdir -p $ORCA_TMPDIR\n',
                '\n',
                '# Setup Orca\n',
                'export ORCA_BIN_DIR=$SHAREDHOMEDIR/orca-4_0_1\n',
                'export PATH=$PATH:$ORCA_BIN_DIR\n',
                '\n',
                '# Copy files \n',
                'cp -f ' + shlexnames['inputfile'] + ' $ORCA_TMPDIR\n\n'])
    out.extend(['cd $ORCA_TMPDIR\n',
                '\n',
                '# Print job info in output file\n',
                'echo "job_id : $SLURM_JOB_ID"\n',
                'echo "job_name : $SLURM_JOB_NAME"\n',
                'echo "node_number : $SLURM_JOB_NUM_NODES nodes"\n',
                'echo "core number : $SLURM_NTASKS cores"\n',
                'echo "Node list : $SLURM_JOB_NODELIST"\n',
                '\n'])
    # runtime is walltime minus one minute (with at least one minute)
    walltime = [int(x) for x in runvalues['walltime'].split(':')]
    runtime = max(3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60, 60)
    out.extend(['# Start ORCA\n',
                'timeout ' + str(runtime) + ' $ORCA_BIN_DIR/orca ' + shlexnames['inputfile'],
                ' > ' + shlexnames['basename'] + '.out\n',
                '\n'])
    out.extend(['# Move files back to original directory\n',
                'cp ' + shlexnames['basename'] + '.out $SLURM_SUBMIT_DIR\n',
                'cp ' + shlexnames['basename'] + '.gbw $SLURM_SUBMIT_DIR\n',
                'cp ' + shlexnames['basename'] + '.xyz $SLURM_SUBMIT_DIR\n',
                '\n'])
    out.extend(['# Empty scratch directory\n',
                'rm -rf $ORCA_TMPDIR\n',
                '\n',
                'echo "Computation finished."\n',
                '\n'])

    # Write .sh file
    with open(output, 'w') as script_file:
        script_file.writelines(out)


def help_description():
    """Return description of program for help message."""
    return """
Setup and submit a job to the SLURM queueing system on the OCCIGEN cluster.
The jobscript name should end with .inp .
"""


def help_epilog():
    """Return additionnal help message."""
    return """
Defaults values:
  Default memory:          64 GB
  Default cores:           24
  Default walltime:        24:00:00

Values for number of cores used and memory to use are read in the input file,
but are overridden if command line arguments are provided.

When using shared nodes (less than 24 cores), the default memory is 5GB per
core.
"""


if __name__ == '__main__':
    main()
