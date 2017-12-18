#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Submit script for Gaussian09 for Computing Clusters.

Script adapted for Idris cluster Ada
Last Update 2017-12-15 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import argparse
import sys
import os
import logging
import shlex
import re


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
        - Cluster section
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
        print("llsubmit {0}".format(shlex.quote(script_file_name)))
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

    # Create run file for gaussian
    create_run_file(script_file_name, runvalues)
    # Submit the script
    os.system('llsubmit {0}'.format(shlex.quote(script_file_name)))
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
    parser.add_argument('-t', '--walltime', default="100:00:00", type=str,
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
                               'gaussian_memory', 'chk', 'oldchk', 'rwf', 'nproc_in_input',
                               'memory_in_input', 'nbo', 'nbo_basefilename'])
    runvalues['inputfile'] = ''
    runvalues['outputfile'] = ''
    runvalues['nodes'] = 1
    runvalues['cores'] = 24
    runvalues['walltime'] = '100:00:00'
    runvalues['memory'] = 4000  # In MB
    runvalues['gaussian_memory'] = 1000  # in MB
    runvalues['chk'] = set()
    runvalues['oldchk'] = set()
    runvalues['rwf'] = set()
    runvalues['nproc_in_input'] = False
    runvalues['memory_in_input'] = False
    runvalues['nbo'] = False
    runvalues['nbo_basefilename'] = ''
    return runvalues


def get_values_from_input_file(input_file, runvalues):
    """Get core/memory values from input file, reading the Mem and NProcShared parameters."""
    with open(input_file, 'r') as file:
        # Go through lines and test if they are containing nproc, mem, etc. related
        # directives.
        for line in file.readlines():
            if "%nproc" in line.lower():
                runvalues['nproc_in_input'] = True
                runvalues['cores'] = int(line.split("=")[1].rstrip('\n'))
            if "%chk" in line.lower():
                runvalues['chk'].add(line.split("=")[1].rstrip('\n'))
            if "%oldchk" in line.lower():
                runvalues['oldchk'].add(line.split("=")[1].rstrip('\n'))
            if "%rwf" in line.lower():
                runvalues['rwf'].add(line.split("=")[1].rstrip('\n'))
            if "%mem" in line.lower():
                runvalues['memory_in_input'] = True
                mem_line = line.split("=")[1].rstrip('\n')
                mem_value, mem_unit = re.match(r'(\d+)([a-zA-Z]+)', mem_line).groups()
                if mem_unit == "GB":
                    runvalues['gaussian_memory'] = int(mem_value) * 1000
                elif mem_unit == "GW":
                    runvalues['gaussian_memory'] = int(mem_value) / 8 * 1000
                elif mem_unit == "MB":
                    runvalues['gaussian_memory'] = int(mem_value)
                elif mem_unit == "MW":
                    runvalues['gaussian_memory'] = int(mem_value) / 8
            if "nbo6" in line.lower() or "npa6" in line.lower():
                runvalues['nbo'] = True
            if "TITLE=" in line:
                # TITLE=FILENAME
                runvalues['nbo_basefilename'] = line.split('=')[1]

    return runvalues


def fill_missing_values(runvalues):
    """Compute and fill all missing values."""
    # TODO Adapt to Ada
    if runvalues['cores'] > 999 and runvalues['nodes'] == 1:
        raise ValueError("Number of cores cannot exceed 28 for one node.")
    elif runvalues['nodes'] > 1:
        raise ValueError("Multiple nodes not supported at the moment.")

    # TODO: manage the multiple nodes case

    # TODO; Better memory checks
    memory, gaussian_memory = compute_memory(runvalues)
    runvalues['memory'] = memory
    runvalues['gaussian_memory'] = gaussian_memory

    return runvalues


def create_shlexnames(runvalues):
    """Return dictionary containing shell escaped names for all possible files."""
    shlexnames = dict()
    input_basename = os.path.splitext(runvalues['inputfile'])[0]
    shlexnames['inputfile'] = shlex.quote(runvalues['inputfile'])
    shlexnames['basename'] = shlex.quote(input_basename)
    if runvalues['chk'] is not None:
        shlexnames['chk'] = [shlex.quote(chk) for chk in runvalues['chk']]
    if runvalues['oldchk'] is not None:
        shlexnames['oldchk'] = [shlex.quote(oldchk) for oldchk in runvalues['oldchk']]
    if runvalues['rwf'] is not None:
        shlexnames['rwf'] = [shlex.quote(rwf) for rwf in runvalues['rwf']]
    return shlexnames


def compute_memory(runvalues):
    """
    Return ideal memory value for Ada.

    2.5GB per core - 1 GB (overhead) in Gaussian, available: 3.5GB per core.
    Computed to use as much as possible the memory available.
    """
    if runvalues['gaussian_memory'] is not None:
        # Memory already defined in input file
        gaussian_memory = runvalues['gaussian_memory']
        # LoadLeveler memory requirement is gaussian_memory + overhead, as long as
        # it fits within the general node requirements
        if gaussian_memory + runvalues['cores'] * 1000 + 1000 < runvalues['cores'] * 3500:
            memory = gaussian_memory + runvalues['cores'] * 1000 + 1000
        else:
            memory = runvalues['cores'] * 3500

    else:
        # Memory not in input, compute everything according to number of cores
        gaussian_memory = runvalues['cores'] * 2500 - 1000
        memory = runvalues['cores'] * 3500

    return (memory, gaussian_memory)


def create_run_file(output, runvalues):
    """
    Create .sh file that contains the script to actually run on the server.

    Structure:
        - LoadLeveler instructions for the queue manager
        - setup of Gaussian09 on the nodes
        - creation of scratch, copy necessary files
        - Run Gaussian09
        - Copy appropriate files back to $HOME
        - Cleanup scratch

    Instructions adapted from www.cines.fr
    """
    # Setup logging
    logger = logging.getLogger()

    # Setup names to use in file
    shlexnames = create_shlexnames(runvalues)
    logger.debug("Runvalues:  %s", runvalues)
    logger.debug("Shlexnames: %s", shlexnames)

    # TODO: multi-nodes
    out = ['#!/bin/bash\n',
           '# @ job_name         = ' + shlexnames['inputfile'] + '\n',
           '# @ output           = ' + shlexnames['basename'] + '.llout\n',
           '# @ error            = ' + shlexnames['basename'] + '.llerr\n',
           '# @ notification     = always\n',
           '# @ notify_user      = user@server.org\n',
           '# @ job_type         = serial\n',
           '# @ parallel_threads = ' + str(runvalues['cores']) + '\n',
           '# @ wall_clock_limit = ' + runvalues['walltime'] + '\n',
           '# @ queue\n',
           '\n']
    out.extend(['# Load Gaussian Module\n',
                'module purge\n',
                'module load gaussian/g09_D01\n',
                '\n',
                '# Setup Gaussian specific variables\n',
                'export g09root\n',
                'source $g09root/g09/bsd/g09.profile\n',
                '\n'])
    if runvalues['nbo']:
        out.extend(['# Setup NBO6\n',
                    'export NBOBIN=$SHAREDHOMEDIR/nbo6/bin\n',
                    'export PATH=$PATH:$NBOBIN\n',
                    '\n'])
    out.extend(['# Setup Scratch\n',
                'export GAUSS_SCRDIR=$TMPDIR\n',
                'mkdir -p $GAUSS_SCRDIR\n',
                '\n',
                '# Copy input file\n',
                'cp -f ' + shlexnames['inputfile'] + ' $GAUSS_SCRDIR\n\n'])
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
                'echo "job_id : $LOADL_STEP_ID"\n',
                'echo "job_name : $LOADL_JOB_NAME"\n',
                'echo "node_number : $LOADL_JOB_NODELIST nodes"\n',
                'echo "core number : $LOADL_TOTAL_TASKS cores"\n',
                '\n'])
    walltime = [int(x) for x in runvalues['walltime'].split(':')]
    runtime = 3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60
    out.extend(['# Start Gaussian\n',
                '( '])
    if not runvalues['nproc_in_input']:  # nproc line not in input
        out.extend('echo %NProcShared=' + str(runvalues['cores']) + '; ')
    if not runvalues['memory_in_input']:  # memory line not in input
        out.extend('echo %Mem=' + str(runvalues['gaussian_memory']) + 'MB ; ')
    out.extend(['cat ' + shlexnames['inputfile'] + ' ) | ',
                'timeout ' + str(runtime) + ' g09 > ',
                '$LOADL_STEP_INITDIR/' + shlexnames['basename'] + '.log\n',
                '\n'])
    out.extend(['# Move files back to original directory\n',
                'cp ' + shlexnames['basename'] + '.log $LOADL_STEP_INITDIR\n',
                '\n'])
    out.extend(['# If chk file exists, create fchk and copy everything\n',
                'for f in $GAUSS_SCRDIR/*.chk; do\n',
                '    [ -f "$f" ] && formchk $f\n',
                'done\n',
                '\n',
                'for f in $GAUSS_SCRDIR/*chk; do\n',
                '    [ -f "$f" ] && cp $f $LOADL_STEP_INITDIR\n',
                'done\n',
                '\n'])
    if runvalues['nbo']:
        out.extend(['# Retrieve NBO Files\n',
                    'cp ' + runvalues['nbo_basefilename'] + '.*'
                    ' $LOADL_STEP_INITDIR\n'
                    '\n'])
    out.extend(['# If Gaussian crashed or was stopped somehow, copy the rwf\n',
                'for f in $GAUSS_SCRDIR/*rwf; do\n',
                '    mkdir -p $HOME/rwf\n'
                # Copy rwf as JobName_123456.rwf
                '    [ -f "$f" ] && cp $f $HOME/rwf/' +
                shlexnames['basename'] + '_$LOADL_STEP_ID.rwf\n',
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
Setup and submit a job to the LoadLeveler queueing system on the Ada cluster.
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
  module load gaussian
  # Source the g09 setup file
  source $g09root/g09/bsd/g09.profile
"""


if __name__ == '__main__':
    main()
