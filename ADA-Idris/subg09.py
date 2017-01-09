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
import re


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
    # Get computation parameters from input file
    runvalues = get_values_from_input_file(input_file_name, runvalues)
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

    runvalues = dict.fromkeys(['inputfile', 'cores', 'walltime', 'memory',
                               'chk', 'oldchk', 'rwf', 'nproc_in_input',
                               'memory_in_input'])
    # Get values from parser
    runvalues['inputfile'] = os.path.basename(args.inputfile[0])
    runvalues['cores'] = args.nproc
    runvalues['walltime'] = args.walltime
    if args.memory:
        runvalues['memory'] = args.memory

    # Initialize empty values
    runvalues['chk'] = set()
    runvalues['oldchk'] = set()
    runvalues['rwf'] = set()
    return runvalues


def get_values_from_input_file(input_file, runvalues):
    """
        Get core/memory values from input file, reading the Mem and
        NProcShared parameters
    """
    with open(input_file, 'r') as file:
        # Go through lines and test if they are containing nproc, mem related
        # directives.
        for line in file.readlines():
            if "%nproc" in line.lower():
                runvalues['nproc_in_input'] = True
                runvalues["cores"] = int(line.split("=")[1].rstrip('\n'))
            if "%chk" in line.lower():
                runvalues["chk"].add(line.split("=")[1].rstrip('\n'))
            if "%oldchk" in line.lower():
                runvalues["oldchk"].add(line.split("=")[1].rstrip('\n'))
            if "%rwf" in line.lower():
                runvalues["rwf"].add(line.split("=")[1].rstrip('\n'))
            if "%mem" in line.lower():
                runvalues['memory_in_input'] = True
                mem_line = line.split("=")[1].rstrip('\n')
                mem_value, mem_unit = re.match(r'(\d+)([a-zA-Z]+)',
                                               mem_line).groups()
                if mem_unit == "GB":
                    runvalues["memory"] = int(mem_value) * 1000
                elif mem_unit == "GW":
                    runvalues["memory"] = int(mem_value) / 8 * 1000
                elif mem_unit == "MB":
                    runvalues["memory"] = int(mem_value)
                elif mem_unit == "MW":
                    runvalues["memory"] = int(mem_value) / 8
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


def create_shlexnames(input_file, runvalues):
    """
        Return dictionary containing shell escaped names for all possible files
    """
    shlexnames = dict()
    input_basename = os.path.splitext(input_file)[0]
    shlexnames['inputname'] = shlex.quote(input_file)
    shlexnames['basename'] = shlex.quote(input_basename)
    if runvalues['chk'] is not None:
        shlexnames['chk'] = [shlex.quote(chk) for chk in runvalues['chk']]
    if runvalues['oldchk'] is not None:
        shlexnames['oldchk'] = [shlex.quote(oldchk) for oldchk in
                                runvalues['oldchk']]
    if runvalues['rwf'] is not None:
        shlexnames['rwf'] = [shlex.quote(rwf) for rwf in runvalues['rwf']]
    return shlexnames


def create_run_file(input_file, output, runvalues):
    """
        Create .sh file that contains the script to actually run on the server.
        Structure:
        -- SBATCH instructions for the queue manager
        -- setup of Gaussian09 on the nodes
        -- creation of scratch, copy necessary files
        -- Run Gaussian09
        -- Copy appropriate files back to $HOME
        -- Cleanup scratch

        Instructions adapted from www.cines.fr
    """

    # Compute memory required:
    # max of 5GB per core (default), or memory from input + 2GB for overhead.
    if runvalues['memory'] is not None:
        if runvalues['memory'] == runvalues['cores'] * 5000:
            memory = runvalues['cores'] * 5000
        else:
            memory = max(runvalues['cores'] * 5000,
                         runvalues['memory'] + 2000)
    else:
        memory = runvalues['cores'] * 5000

    # Setup names to use in file
    shlexnames = create_shlexnames(input_file, runvalues)

    out = ['#!/bin/bash\n',
           '#SBATCH -J ' + shlexnames['inputname'] + '\n',
           '#SBATCH --mail-type=ALL\n',
           '#SBATCH --mail-user=user@server.org\n',
           '#SBATCH --nodes=1\n',
           '#SBATCH --ntasks=' + str(runvalues['cores']) + '\n',
           '#SBATCH --mem=' + str(memory) + '\n',
           '#SBATCH --time=' + runvalues['walltime'] + '\n',
           '#SBATCH --output=' + shlexnames['basename'] + '.slurmout\n',
           '\n']
    out.extend(['# Load Gaussian Module\n',
                'module purge\n',
                'module load gaussian\n',
                '\n',
                '# Setup Gaussian specific variables\n',
                'export g09root\n',
                'source $g09root/g09/bsd/g09.profile\n',
                'export OMP_NUM_THREADS=$SLURM_NPROCS\n',
                '\n'])
    out.extend(['# Setup Scratch\n',
                'export GAUSS_SCRDIR=$SCRATCHDIR/gaussian/$SLURM_JOBID\n',
                'mkdir -p $GAUSS_SCRDIR\n',
                '\n',
                '# Copy input file\n',
                'cp -f ' + shlexnames['inputname'] + ' $GAUSS_SCRDIR\n\n'])
    # If chk file is defined in input and exists, copy it in scratch
    if runvalues['chk'] is not None:
        out.extend('# Copy chk file in scratch if it exists\n')
        for chk in shlexnames['chk']:
            out.extend(['if [ -f ' + chk + ' ] \n',
                        'then\n',
                        '  cp ' + chk + ' $GAUSS_SCRDIR\n',
                        'fi\n\n'])
    # If oldchk file is defined in input and exists, copy it in scratch
    if runvalues['oldchk'] is not None:
        out.extend('# Copy oldchk file in scratch if it exists\n')
        for oldchk in shlexnames['oldchk']:
            out.extend(['if [ -f ' + oldchk + ' ] \n',
                        'then\n',
                        '  cp ' + oldchk + ' $GAUSS_SCRDIR\n',
                        'fi\n\n'])
    # If rwf file is defined in input and exists, copy it in scratch
    if runvalues['rwf'] is not None:
        out.extend('# Copy rwf file in scratch if it exists\n')
        for rwf in shlexnames['rwf']:
            out.extend(['if [ -f ' + rwf + ' ] \n',
                        'then\n',
                        '  cp ' + rwf + ' $GAUSS_SCRDIR\n',
                        'fi\n\n'])
    out.extend(['cd $GAUSS_SCRDIR\n',
                '\n',
                '# Print job info in output file\n',
                'echo "job_id : $SLURM_JOBID"\n',
                'echo "job_name : $SLURM_JOB_NAME"\n',
                'echo "node_number : $SLURM_NNODES nodes"\n',
                'echo "core number : $SLURM_NPROCS cores"\n',
                '\n'])
    walltime = [int(x) for x in runvalues['walltime'].split(':')]
    runtime = 3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60
    out.extend(['# Start Gaussian\n',
                '( '])
    if runvalues['nproc_in_input'] is None:  # nproc line not in input
        out.extend('echo %NProcShared=' + str(runvalues['cores']) + '; ')
    if runvalues['memory_in_input'] is None:  # memory line not in input
        # Use memory minus 1GB per core to account for Gaussian overhead
        gaussian_memory = memory - min(6000, 1000 * runvalues['cores'])
        out.extend('echo %Mem=' + str(gaussian_memory) + 'MB ; ')
    out.extend(['cat ' + shlex.quote(input_file) + ' ) | ',
                'timeout ' + str(runtime) + ' g09 > ',
                '' + shlexnames['basename'] + '.log\n',
                '\n'])
    out.extend(['# Move files back to original directory\n',
                'cp ' + shlexnames['basename'] + '.log $SLURM_SUBMIT_DIR\n',
                '\n'])
    out.extend(['# If chk file exists, create fchk and copy everything\n',
                'for f in $GAUSS_SCRDIR/*.chk; do\n',
                '    [ -f "$f" ] && formchk $f\n',
                'done\n',
                '\n',
                'for f in $GAUSS_SCRDIR/*chk; do\n',
                '    [ -f "$f" ] && cp $f $SLURM_SUBMIT_DIR\n',
                'done\n',
                '\n',
                '\n'])
    out.extend(['# If Gaussian crashed or was stopped somehow, copy the rwf\n',
                'for f in $GAUSS_SCRDIR/*rwf; do\n',
                '    mkdir -p $SCRATCHDIR/gaussian/rwf\n'
                # Copy rwf as JobName_123456.rwf
                '    [ -f "$f" ] && cp $f $SCRATCHDIR/gaussian/rwf/' +
                shlexnames['basename'] + '_$SLURM_JOBID.rwf\n',
                'done\n',
                '\n',
                '# Empty Scratch directory\n',
                'rm -rf $GAUSS_SCRDIR\n',
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
