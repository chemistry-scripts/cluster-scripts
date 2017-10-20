#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Submit script for Gaussian09 for Computing Clusters
Original script for LISA by Jos Mulder
email j.r.mulder -at- vu.nl
Adapted for Cines OCCIGEN cluster
Last Update 2016-02-04 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import argparse
import sys
import os
import shlex


def main():
    """
        Main function.
        Checks existence of input, and non-existence of batch file (e.g.
        computation has not already been submitted).
        Run computation
    """
    # Get parameters from command line
    runvalues = get_options()
    # Retrieve input file name, create output file name
    input_file_name = runvalues['inputfile']
    output = input_file_name + ".sh"
    # Check existence of input file
    if not os.path.exists(input_file_name):
        print("=========== !!! WARNING !!! ===========")
        print("The input file was not found.")
        sys.exit()
    # Check that file input.sh does not exist, not to start twice the same job
    if os.path.exists(output):
        print("=========== !!! WARNING !!! ===========")
        print(" The corresponding .sh file already exists ")
        print("Make sure it is not a mistake, erase it and rerun the script")
        print("Alternatively, you can submit the job directly with:")
        print("sbatch {0}".format(shlex.quote(output)))
        sys.exit()
    # Avoid end of line problems due to conversion between Windows and Unix
    # file endings
    os.system('dos2unix {0}'.format(shlex.quote(input_file_name)))
    # Backfill parameters not set in the runvalues
    runvalues = get_defaultvalues(runvalues)
    # Create run file for gaussian
    create_run_file(input_file_name, output, runvalues)
    # Submit the script
    os.system('sbatch {0}'.format(shlex.quote(output)))
    print("job {0} submitted with a walltime of {1} hours"
          .format(input_file_name, runvalues["walltime"]))


def get_options():
    """
        Check command line options and accordingly set computation parameters
    """
    parser = argparse.ArgumentParser(description=help_description(),
                                     epilog=help_epilog())
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.add_argument('-n', '--nproc', default=24, type=int,
                        help="Number of cores used for the computation")
    parser.add_argument('-t', '--walltime', default="24:00:00", type=str,
                        help="Maximum time allowed for the computation")
    parser.add_argument('-m', '--memory', type=int,
                        help="Amount of memory allowed for the computation")
    parser.add_argument('inputfile', type=str, nargs='+',
                        help='The input file to submit')

    try:
        args = parser.parse_args()
    except argparse.ArgumentError as error:
        print(str(error))  # Print something like "option -a not recognized"
        sys.exit(2)

    runvalues = dict.fromkeys(['inputfile', 'cores', 'walltime', 'memory'])
    # Get values from parser
    runvalues['inputfile'] = os.path.basename(args.inputfile[0])
    runvalues['cores'] = args.nproc
    runvalues['walltime'] = args.walltime

    return runvalues


def get_defaultvalues(runvalues):
    """
        Fill the runvalues table with the default values in case they are not
        existing already.
    """
    if runvalues['memory'] is None:
        if runvalues['cores'] == 24:  # Single node, allow 64GB nodes
            runvalues['memory'] = 58000
        else:  # Shared nodes
            runvalues['memory'] = 5000 * runvalues['cores']
    return runvalues


def create_shlexnames(input_file):
    """
        Return dictionary containing shell escaped names for all possible files
    """
    shlexnames = dict()
    input_basename = os.path.splitext(input_file)[0]
    shlexnames['inputname'] = shlex.quote(input_file)
    shlexnames['basename'] = shlex.quote(input_basename)
    return shlexnames


def create_run_file(input_file, output, runvalues):
    """
        Create .sh file that contains the script to actually run on the server.
        Structure:
        -- SBATCH instructions for the queue manager
        -- setup of ADF on the nodes
        -- creation of scratch, copy necessary files
        -- Run ADF
        -- Copy and rename appropriate files back to $HOME
        -- Cleanup scratch

        Instructions adapted from www.cines.fr
    """

    # Setup names to use in file
    shlexnames = create_shlexnames(input_file)

    # FROM OCCIGEN Reference:
    # =====> export OMP_NUM_THREADS=24
    # =====> export KMP_AFFINITY=compact,1,0
    # =====> Start ORCA with full path
    out = ['#!/bin/bash\n',
           '#SBATCH -J ' + shlexnames['inputname'] + '\n',
           '#SBATCH --mail-type=ALL\n',
           '#SBATCH --mail-user=user@server.org\n',
           '#SBATCH --constraint=HSW24\n',
           '#SBATCH --nodes=1\n',
           '#SBATCH --ntasks=' + str(runvalues['cores']) + '\n',
           '#SBATCH --mem=' + str(runvalues['memory']) + '\n',
           '#SBATCH --time=' + runvalues['walltime'] + '\n',
           '#SBATCH --output=' + shlexnames['basename'] + '.slurmout\n',
           '\n']
    out.extend(['# Load ADF Module\n',
                'module purge\n',
                'module load intel/17.2\n',
                'module load openmpi/intel/2.0.2\n',
                'module load adf/2016.102-openmpi',
                '\n'])

    out.extend(['# Setup Scratch\n',
                'export SCM_TMPDIR=$SCRATCHDIR/adf/$SLURM_JOBID\n',
                'mkdir -p $SCM_TMPDIR\n',
                '\n',
                '# Copy files \n',
                'cp -f ' + shlexnames['inputname'] + ' $SCM_TMPDIR\n\n'])
    out.extend(['cd $SCM_TMPDIR\n',
                '\n',
                '# Print job info in output file\n',
                'echo "job_id : $SLURM_JOBID"\n',
                'echo "job_name : $SLURM_JOB_NAME"\n',
                'echo "node_number : $SLURM_NNODES nodes"\n',
                'echo "core number : $SLURM_NPROCS cores"\n',
                '\n'])
    # runtime is walltime minus one minute (with at least one minute)
    walltime = [int(x) for x in runvalues['walltime'].split(':')]
    runtime = max(3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60, 60)
    out.extend(['# Start ADF\n',
                'timeout ' + str(runtime) + ' sh < ' + shlex.quote(input_file),
                ' > ' + shlexnames['basename'] + '.out\n',
                '\n'])
    out.extend(['# Move files back to original directory\n',
                'cp ' + shlexnames['basename'] + '.out $SLURM_SUBMIT_DIR\n',
                '\n'])
    out.extend(['# If ADF crashed or was stopped somehow, copy the t13\n',
                'if [ -f TAPE13 ];\n',
                '    then mv TAPE13 $SLURM_SUBMIT_DIR/',
                shlexnames['basename'] + '.t13 \n',
                'fi\n',
                '# If ADF crashed or was stopped somehow, copy the t10\n',
                'if [ -f TAPE10 ];\n',
                '    then mv TAPE10 $SLURM_SUBMIT_DIR/',
                shlexnames['basename'] + '.t10 \n',
                'fi\n',
                '# Copy and rename the TAPE21 file\n',
                'if [ -f TAPE21 ];\n',
                '    then mv TAPE21 $SLURM_SUBMIT_DIR/',
                shlexnames['basename'] + '.t21 \n',
                'fi\n',
                '# Copy and rename the logfile\n',
                'mv logfile $SLURM_SUBMIT_DIR/',
                shlexnames['basename'] + '.logfile\n',
                '\n',
                '# Empty Scratch directory\n',
                'rm -rf $SCM_TMPDIR\n',
                '\n',
                'echo "Computation finished."\n',
                '\n'])

    # Write .sh file
    with open(output, 'w') as script_file:
        script_file.writelines(out)


def help_description():
    """
        Returns description of program for help message
    """
    return """
Setup and submit a job to the SLURM queueing system on the OCCIGEN cluster.
The jobscript name should end with .gjf or .com.
"""


def help_epilog():
    """
        Returns additionnal help message
    """
    return """
Defaults values:
  Default memory:          64 GB
  Default cores:           24
  Default walltime:        24:00:00

Values for number of cores used and memory to use are read in the input file,
but are overridden if command line arguments are provided.

When using shared nodes (less than 24 cores), the default memory is 5GB per
core.

To copy in bashrc:
  ##### Gaussian 2009
  # Load the gaussian module which will set the variable g09root.
  module load gaussian
  # Source the g09 setup file
  source $g09root/g09/bsd/g09.profile
"""


if __name__ == '__main__':
    main()
