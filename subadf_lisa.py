#!/usr/bin/env python
# Submit script for ADF2014 for the lisa cluster
# This adds automatic moving of TAPE21 file, mail at beginning and end of computation
# to the user, and a few others details, such as automatic selection of queue and max time.
# Created by Emmanuel Nicolas
# email e.c.nicolas -at- vu.nl

import getopt, sys, os, fileinput

StartupFlag = "###--- File automatically edited by subadf script ---###"

## the main function calls the functions in the right order
def main():
    runvalues = getdefaults()
    # Retrieve input file name
    input = str(getinput(sys.argv[-1]))
    # Avoid end of line problems due to conversion between Windows and Unix file endings
    os.system('dos2unix %s' %input)
    runvalues = flagcontrol(runvalues, sys.argv[0:-1])
    # Save current path
    pathname = os.getcwd()
    # Test if inputfile has not already been submitted
    with open(input, "r") as inFile:
        if inFile.readline().startswith(StartupFlag):
            print("========= WARNING !!!!! ===========")
            print("The file has already been edited.")
            print("Either delete edits, or submit it straight through the following command line:")
            print("qsub {jobfile}".format(jobfile=input))
            sys.exit()
    # Prep input file
    prepFile(input,pathname,runvalues)

    # Submit computation
    os.system("qsub {jobfile}".format(jobfile=input))
    print("Job {jobfile} submitted on queue {queue}  with a walltime of {walltime} hours".format(queue=runvalues["queue"], jobfile=input, walltime=runvalues["walltime"]))

#### the default values ########
def getdefaults():
    return {"walltime" : "120:00", "nodes" : "1", "queue" : "p16"}

### checks if the input file exists #####
def getinput(input):
    if input == "--help":
        print(help())
        sys.exit()
    elif input  == "-h":
        print(help())
        sys.exit()
    else:
        if not os.path.exists(input):
            print("The job script file given does not exist")
            sys.exit()
        else:
            return input

## This function checks the flags and adjust the default values  ##
def flagcontrol(runvalues, flaglist):
    try:
        opts, args = getopt.getopt(flaglist[1:], "v:q:n:t:")
    except getopt.GetoptError as err:
        print(str(err)) # will print something like "option -a not recognized"
        sys.exit(2)
    for o, a in opts:
        if o == "-n":
            runvalues["nodes"] = a
        elif o == "-q":
            runvalues["queue"] = a
        elif o == "-t":
            runvalues["walltime"] = a
        else:
            assert False, "unhandled option"
    return runvalues

## This function modifies the input file, inserting mail functions and TAPE21 saving
def prepFile(input,pathname,runvalues):
    tmpFile = input + ".tmp"
    mailBeginning = """echo "Computation $PBS_JOBID (Job: {job}) started at `date`." | mail $USER -s "Job $PBS_JOBID"\n""".format(job=input)
    mailFinished  = """echo "Computation $PBS_JOBID (Job: {job}) completed at `date`." | mail $USER -s "Job $PBS_JOBID"\n""".format(job=input)
    with open(input, "r") as inFile:
        with open(tmpFile, "w+") as outFile: # Need a try statement if file already exists, or use tempfile mechanisms from python3
            # Add Flag at start, indicating that the file has been edited already, not to start it twice
            outFile.write(StartupFlag+'\n')

            # Setup PBS Commands
            outFile.write("#PBS -lnodes={nodes}:ppn=16\n".format(nodes=runvalues["nodes"]))
            outFile.write("#PBS -S /bin/bash\n")
            outFile.write("#PBS -lwalltime="+ runvalues["walltime"]+":00\n")

            # Modules, Folders, etc.
            outFile.write(lisaSpecifics())

            # Mail at the beginning of the job
            outFile.write(mailBeginning)

            # Main adf input file, adding the output file
            for i in inFile:
                if i.find("<<eor") == -1:
                    outFile.write(i)
                    continue
                else:
                    # Line containing adf command > add output redirection to file instead of stdout. >> Required to cat all files in case of multiple jobs
                    i = i.strip()
                    i += " >> {fileName}.out\n".format(fileName=input[:-4])
                    outFile.write(i)

            # move TAPE13 file if it exists (for failed calculations)
            t13File = pathname + "/" + input[:-3] + "t13"
            outFile.write("if [ -f TAPE13 ];\n")
            outFile.write("then mv TAPE13 {t13}\n".format(t13=t13File))
            outFile.write("fi\n")
            # move TAPE10 file if it exists (for failed calculations)
            t10File = pathname + "/" + input[:-3] + "t10"
            outFile.write("if [ -f TAPE10 ];\n")
            outFile.write("then mv TAPE10 {t10}\n".format(t10=t10File))
            outFile.write("fi\n")
            # move TAPE21 file
            t21File = pathname + "/" + input[:-3] + "t21"
            outFile.write("if [ -f TAPE21 ];\n")
            outFile.write("then mv TAPE21 {t21}\n".format(t21=t21File))
            outFile.write("fi\n")
            # move logfile
            logFile = pathname + "/" + input[:-3] + "logfile"
            outFile.write("mv logfile {log}\n".format(log=logFile))
            # Mail at the end of the job
            outFile.write(mailFinished)
    os.rename(tmpFile, input)

def lisaSpecifics():
    return """
if [ -z "$PBS_NODEFILE" ]
then
    >&2 echo ERROR: ADF should not be run interactively.
    >&2 echo ERROR: Please use \"qsub $0 \" to submit this script as a batch job.
    exit 1
fi

# Load the appropriate modules for running ADF 2014.
# paffinity is to ensure that processes are bound to cores

module load adf/2012.01
module load paffinity

# set the stacksize limit to 1 GB
ulimit -s 1048576

# Compute the number of cores from the PBS nodefile
# Set the ADF dependent environment variable NSCM to the number of tasks

export NSCM=`wc -l < $PBS_NODEFILE`

# ADF specific environment:
export SCM_MACHINEFILE=$PBS_NODEFILE
export SCM_TMPDIR="$TMPDIR"
export SCM_USETMPDIR=yes

# for the MPI environment:
export NETWORK=ib
export MPIVERS=hpmpi

# Change the current directory to where the batch job was submitted.
cd $PBS_O_WORKDIR

"""

def help():
    return   """Usage
-----
subadf.py [options] jobscript

Description
-----------
Submit a job to the batch queueing system.

Options
-------
-h,--help                Print usage message.
-n <value>               The number of nodes to run on.
-q <value>               Queue to submit job to.
-t <value>               Wall Clock time limit (format h:m).

Queues
------
QUEUE                 ALIASES        DESCRIPTION
cores12               p12            Dual-processor Hexa-Core Intel Xeon. Time limit 120:00.
cores16               p16            Dual-Processor 8-Core Intel Sandy-Bride. Time limit 120:00.

Defaults
-----------------------
Default Queue:           p16
Default nodes:           1
Default walltime:        120:00

Information for the  .bashrc
-----------------------

"""

if __name__ == '__main__': main()
