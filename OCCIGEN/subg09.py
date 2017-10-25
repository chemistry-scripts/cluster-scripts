#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Submit script for Gaussian09 for Computing Clusters.

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
    Set up the computation and submit it to the job scheduler.

    Checks existence of input, and non-existence of batch file
    (e.g. computation has not already been submitted).
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
    # If NBO required, retrieve NBO values
    if runvalues['nbo']:
        get_nbo_values(input_file_name, runvalues)
    # Create run file for gaussian
    create_run_file(input_file_name, output, runvalues)
    # Submit the script
    os.system('sbatch {0}'.format(shlex.quote(output)))
    print("job {0} submitted with a walltime of {1} hours"
          .format(input_file_name, runvalues["walltime"]))


def get_options():
    """Check command line options and accordingly set computation parameters."""
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
    runvalues['nbo'] = False
    runvalues['nbo_basefilename'] = ''

    return runvalues


def get_values_from_input_file(input_file, runvalues):
    """Get core/memory values from input file, reading the Mem and NProcShared parameters."""
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
            if "nbo" in line.lower():
                runvalues['nbo'] = True

    # Setup cluster_section according to number of cores
    if runvalues["cores"] <= 24:
        runvalues['cluster_section'] = "HSW24"
    elif runvalues["cores"] <= 28:
        runvalues['cluster_section'] = "BDW28"
    else:
        raise ValueError("Number of cores cannot exceed 28")
    return runvalues


def get_nbo_values(input_file, runvalues):
    """Retrieve specific NBO parameters from the input."""
    with open(input_file, 'r') as file:
        # Go through lines and test if they are containing nproc, mem related
        # directives.
        for line in file.readlines():
            if "nbo" in line.lower():
                # Should be already set but you never know...
                runvalues['nbo'] = True
            if "TITLE=" in line:
                # TITLE=FILENAME
                runvalues['nbo_basefilename'] = line.split('=')[1]
    return runvalues


def create_shlexnames(input_file, runvalues):
    """Return dictionary containing shell escaped names for all possible files."""
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


def compute_memory(runvalues):
    """
    Return ideal memory value for OCCIGEN.

    4GB per core, or memory from input + 4000 MB for overhead.
    Computed to use as close as possible the memory available.
    """
    if runvalues['memory'] is not None:
        # Memory already defined in input file
        gaussian_memory = runvalues['memory']
        # SLURM memory requirement is gaussian_memory + overhead, as long as
        # it fits within the general node requirements
        if gaussian_memory + 4000 < runvalues['cores'] * 4800:
            memory = gaussian_memory + 4000
        else:
            memory = runvalues['cores'] * 4800
    else:
        gaussian_memory = runvalues['cores'] * 4000
        memory = runvalues['cores'] * 4800

    return (memory, gaussian_memory)


def create_run_file(input_file, output, runvalues):
    """
    Create .sh file that contains the script to actually run on the server.

    Structure:
        - SBATCH instructions for the queue manager
        - setup of Gaussian09 on the nodes
        - creation of scratch, copy necessary files
        - Run Gaussian09
        - Copy appropriate files back to $HOME
        - Cleanup scratch

    Instructions adapted from www.cines.fr
    """
    # Compute memory requirements:
    memory, gaussian_memory = compute_memory(runvalues)

    # Setup names to use in file
    shlexnames = create_shlexnames(input_file, runvalues)

    out = ['#!/bin/bash\n',
           '#SBATCH -J ' + shlexnames['inputname'] + '\n',
           '#SBATCH --constraint=' + runvalues['cluster_section'] + '\n'
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
                'export OMP_NUM_THREADS=$SLURM_NTASKS\n',
                '\n'])
    if runvalues['nbo']:
        out.extend(['# Setup NBO6\n',
                    'export NBOBIN=$SHAREDHOMEDIR/nbo6/bin\n',
                    'export PATH=$PATH:$NBOBIN\n',
                    '\n'])
    out.extend(['# Setup Scratch\n',
                'export GAUSS_SCRDIR=$SCRATCHDIR/gaussian/$SLURM_JOB_ID\n',
                'mkdir -p $GAUSS_SCRDIR\n',
                '\n',
                '# Copy input file\n',
                'cp -f ' + shlexnames['inputname'] + ' $GAUSS_SCRDIR\n\n'])
    # If chk file is defined in input and exists, copy it in scratch
    if runvalues['chk'] != set():
        out.extend('# Copy chk file in scratch if it exists\n')
        for chk in shlexnames['chk']:
            out.extend(['if [ -f ' + chk + ' ] \n',
                        'then\n',
                        '  cp ' + chk + ' $GAUSS_SCRDIR\n',
                        'fi\n\n'])
    # If oldchk file is defined in input and exists, copy it in scratch
    if runvalues['oldchk'] != set():
        out.extend('# Copy oldchk file in scratch if it exists\n')
        for oldchk in shlexnames['oldchk']:
            out.extend(['if [ -f ' + oldchk + ' ] \n',
                        'then\n',
                        '  cp ' + oldchk + ' $GAUSS_SCRDIR\n',
                        'fi\n\n'])
    # If rwf file is defined in input and exists, copy it in scratch
    if runvalues['rwf'] != set():
        out.extend('# Copy rwf file in scratch if it exists\n')
        for rwf in shlexnames['rwf']:
            out.extend(['if [ -f ' + rwf + ' ] \n',
                        'then\n',
                        '  cp ' + rwf + ' $GAUSS_SCRDIR\n',
                        'fi\n\n'])
    out.extend(['cd $GAUSS_SCRDIR\n',
                '\n',
                '# Print job info in output file\n',
                'echo "job_id : $SLURM_JOB_ID"\n',
                'echo "job_name : $SLURM_JOB_NAME"\n',
                'echo "node_number : $SLURM_JOB_NUM_NODES nodes"\n',
                'echo "core number : $SLURM_NTASKS cores"\n',
                'echo "Node list : $SLURM_JOB_NODELIST"\n',
                '\n'])
    walltime = [int(x) for x in runvalues['walltime'].split(':')]
    runtime = 3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60
    out.extend(['# Start Gaussian\n',
                '( '])
    if runvalues['nproc_in_input'] is None:  # nproc line not in input
        out.extend('echo %NProcShared=' + str(runvalues['cores']) + '; ')
    if runvalues['memory_in_input'] is None:  # memory line not in input
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
                '\n'])
    if runvalues['nbo']:
        out.extend(['# Retrieve NBO Files\n',
                    'cp ' + runvalues['nbo_basefilename'] + '.*'
                    ' $SLURM_SUBMIT_DIR\n'
                    '\n'])
    out.extend(['# If Gaussian crashed or was stopped somehow, copy the rwf\n',
                'for f in $GAUSS_SCRDIR/*rwf; do\n',
                '    mkdir -p $SCRATCHDIR/gaussian/rwf\n'
                # Move rwf as JobName_123456.rwf to the rwf folder in scratch
                '    [ -f "$f" ] && mv $f $SCRATCHDIR/gaussian/rwf/' +
                shlexnames['basename'] + '_$SLURM_JOB_ID.rwf\n',
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
    """Return description of program for help message."""
    return """
Setup and submit a job to the SLURM queueing system on the OCCIGEN cluster.
The jobscript name should end with .gjf or .com.
"""


def help_epilog():
    """Return additionnal help message."""
    return """
Defaults values:
  Default memory:          1 GB
  Default cores:           1
  Default walltime:        24:00:00

Values for number of cores used and memory to use are read in the input file,
but are overridden if command line arguments are provided.

When using shared nodes (less than 24 cores), the default memory is 5GB per
core.

To copy in bashrc:
  ##### Gaussian 2009
  # Load the gaussian module which will set the variable g09root.
  module load gaussian/g09
  # Source the g09 setup file
  source $g09root/g09/bsd/g09.profile
"""


if __name__ == '__main__':
    main()
