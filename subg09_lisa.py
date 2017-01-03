#!/usr/bin/env python3

# Submit script for Gaussian 2003 and 2009 for the lisa cluster
# Created by Jos Mulder
# email j.r.mulder -at- vu.nl
# Last Update 2015-11-25 by Emmanuel Nicolas
# email e.c.nicolas -at- vu.nl

import getopt
import sys
import os
import fileinput
import shlex


def main():
    """
        Main function.
        Checks existence of input, and non-existence of batch file (e.g.
        computation has not already been submitted).
        Run computation
    """
    runvalues = getdefaults()
    # Retrieve input file name
    input = str(getinput(sys.argv[-1]))
    # Check presence of input file
    if "subg09.py" in input:
        print("========= WARNING !!!!! ===========")
        print("There is no input file.")
        print("Please rerun the script with a proper file")
        sys.exit()
    # Check that file input.sh does not exist,
    # to avoid starting twice the same job
    if os.path.exists('%s.sh' % input):
        print("========= WARNING !!!!! ===========")
        print(" The corresponding .sh file already exists ")
        print("Make sure it is not a mistake, erase it and rerun the script")
        sys.exit()
    # Avoid end of line problems due to conversion between Windows and Unix
    # file endings
    os.system('dos2unix {0}'.format(shlex.quote(input)))
    runvalues = flagcontrol(runvalues, sys.argv[0:-1])
    # Save current path
    pathname = os.getcwd()
    # Create run file for gaussian
    createRunFile(runvalues, input, pathname)
    # Submit the script
    os.system('qsub {0}'.format(shlex.quote(input+'.sh')))
    print("job {0} submitted with a walltime of {1} hours".format(input,
          runvalues["walltime"]))


def getdefaults():
    """
        Default Computation values
    """
    return {"walltime": "120:00:00",
            "nodes": "1",
            "version": "g09"}


def getinput(input):
    """
        Check existence of input file
    """
    if input == "--help":
        print(help())
        sys.exit()
    elif input == "-h":
        print(help())
        sys.exit()
    else:
        if not os.path.exists(input):
            print("The job script file given does not exist")
            sys.exit()
        else:
            return input


def flagcontrol(runvalues, flaglist):
    """
        Check command line options and accordingly set computation parameters
    """
    try:
        opts, args = getopt.getopt(flaglist[1:], "v:q:n:t:")
    except getopt.GetoptError as err:
        print(str(err))  # will print something like "option -a not recognized"
        sys.exit(2)
    for o, a in opts:
        if o == "-n":
            runvalues["nodes"] = a
        elif o == "-t":
            runvalues["walltime"] = a
        else:
            assert False, "unhandled option"
    return runvalues


def createRunFile(runvalues, input, pathname):
    """
        Create .sh file that contains the script to actually run on the server.
        Structure:
        -- PBS instructions for the queue manager
        -- setup of Gaussian09 on the nodes
        -- Run Gaussian09

        Instructions adapted from www.surfsara.nl/lisa/software/gaussian
    """

    f = ['#PBS -S /bin/bash\n',
         '#PBS -lwalltime=' + runvalues["walltime"] + '\n',
         '#PBS -lnodes=' + runvalues["nodes"] + ':ppn=1\n']
    f.extend(['\n',
              'echo "Computation $PBS_JOBID (Job:' + input + ') started at ',
              '`date`." | mail $USER -s "Job $PBS_JOBID"\n',
              '\n'])
    f.extend(['# Load Modules for Gaussian09 Rev D.01, and nbo 6.\n',
              'module load fortran/intel c/intel mkl nbo g09/D01\n',
              'module load sara-batch-resources\n',
              '\n',
              '#Set up Work Directory\n'
              'rundir=${PBS_O_WORKDIR:-.}\n',
              'cd $rundir\n'
              '\n'])
    f.extend(['# Setup Scratch\n',
              'export GAUSS_SCRDIR=${TMPDIR:-/scratch}/$USER\n',
              'rm -rf $GAUSS_SCRDIR\n',
              'mkdir -p $GAUSS_SCRDIR\n',
              '\n',
              '# Retrieve numproc value for given node\n',
              'numproc=`cat /proc/cpuinfo | grep processor | wc -l`\n',
              '\n'])
    f.extend(['# Setup run variables\n',
              'export GAUSS_LFLAGS=" "\n'
              'export OMP_NUM_THREADS=$numproc\n'
              '\n'])
    walltime = [int(x) for x in runvalues['walltime'].split(':')]
    runtime = 3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60
    f.extend(['# Start Gaussian\n',
              '( echo %NProcShared=${numproc}; ',
              'cat ' + shlex.quote(input) + ' ) | ',
              'timeout ' + str(runtime) + ' g09 > ',
              '' + shlex.quote(input[:-4]) + '.log\n',
              '\n'])
    f.extend(['# If chk file exists, create fchk\n',
              'if [ -f ' + shlex.quote(input[:-4]) + '.chk ] \n',
              'then\n',
              '  formchk ' + shlex.quote(input[:-4]) + '.chk\n',
              'fi\n',
              '\n',
              '# If Gaussian crashed or was stopped somehow, copy the rwf\n'
              'for f in $GAUSS_SCRDIR/*rwf; do\n'
              '    [ -f "$f" ] && cp $f $PBS_O_WORKDIR\n'
              'done\n'
              '# Empty Scratch directory\n'
              'rm -rf $GAUSS_SCRDIR\n'
              '\n'])
    f.extend(['\n',
              'echo "Computation $PBS_JOBID (Job:' + input + ') finished at ',
              '`date`." | mail $USER -s "Job $PBS_JOBID"\n',
              '\n'])

    # Write .sh file
    with open(input + '.sh', 'w') as scriptFile:
        scriptFile.writelines(f)


def help():
    return """Usage
-----
subg09.py [options] jobscript

Description
-----------
Submit a job to the batch queueing system.

Options
-------
-h,--help                Print usage message.
-n <value>               The number of nodes to run on.
-t <value>               Wall Clock time limit (format hh:mm:ss).

-----------------------
Default Application:     g09
Default Queue:           first available
Default nodes:           1, using all proc on node
Default walltime:        120:00

Information for the  .bashrc
-----------------------
##### Gaussian 2009
# Make sure that the module command is active
. /sara/sw/modules/module/init/bash
# Load the g09 module which will set the variable g09root.
module load g09
# Source the g09 setup file
source ${g09root}/g09/bsd/g09.profile

"""

if __name__ == '__main__':
    main()
