#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Helper functions to set up theoretical chemistry computations on computing clusters.

Last Update 2022-01-14 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import os
import shlex
import logging
import re
from math import ceil


class Computation:
    """
    Class representing the computation that will be run.
    """

    def __init__(self, input_file, software, cmdline_args):
        """
        Class Builder
        """
        logger = logging.getLogger()
        self.__input_file = input_file
        self.__software = software
        self.__runvalues = self.default_run_values()
        logger.debug("Runvalues default:  %s", self.runvalues)
        self.fill_from_commandline(cmdline_args)
        logger.debug("Runvalues cmdline:  %s", self.runvalues)
        if self.__software == "g16":
            self.get_values_from_gaussian_input_file()
        elif self.__software == "orca":
            self.get_values_from_orca_input_file()
        logger.debug("Runvalues gaussian:  %s", self.runvalues)
        self.fill_missing_values()
        logger.debug("Runvalues backfilled:  %s", self.runvalues)
        self.shlexnames = self.create_shlexnames()

    @property
    def runvalues(self):
        """Dict containing all data required to run the computation."""
        return self.__runvalues

    @runvalues.setter
    def runvalues(self, value):
        self.__runvalues = value

    @property
    def input_file(self):
        """Input File name."""
        return self.__input_file

    @input_file.setter
    def input_file(self, value):
        self.__input_file = value

    @property
    def software(self):
        """Software name."""
        return self.__software

    @software.setter
    def software(self, value):
        self.__software = value

    @property
    def walltime(self):
        """Walltime."""
        return self.__runvalues["walltime"]

    @staticmethod
    def default_run_values():
        """Fill default runvalues."""
        # Setup runvalues
        runvalues = dict.fromkeys(
            [
                "inputfile",
                "outputfile",
                "nodes",
                "cores",
                "walltime",
                "memory",
                "gaussian_memory",
                "chk",
                "oldchk",
                "rwf",
                "nproc_in_input",
                "memory_in_input",
                "nbo",
                "nbo_basefilename",
                "cluster_section",
            ]
        )
        runvalues["inputfile"] = ""
        runvalues["outputfile"] = ""
        runvalues["nodes"] = 1
        runvalues["cores"] = ""
        runvalues["walltime"] = "24:00:00"
        runvalues["memory"] = 4000  # In MB
        runvalues["gaussian_memory"] = 1000  # in MB
        runvalues["nproc_in_input"] = False
        runvalues["memory_in_input"] = False
        runvalues["nbo"] = False
        runvalues["nbo_basefilename"] = ""
        runvalues["extra_files"] = list()
        return runvalues

    def get_values_from_gaussian_input_file(self):
        """Get core/memory values from input file, reading the Mem and NProcShared parameters."""
        with open(self.input_file, "r") as file:
            # Go through lines and test if they contain nproc, mem, etc. related
            # directives.
            for line in file.readlines():
                if "%nproc" in line.lower():
                    self.runvalues["nproc_in_input"] = True
                    self.runvalues["cores"] = int(line.split("=")[1].rstrip("\n"))
                if "%chk" in line.lower():
                    self.runvalues["extra_files"].append(
                        line.split("=")[1].rstrip("\n")
                    )
                if "%oldchk" in line.lower():
                    self.runvalues["extra_files"].append(
                        line.split("=")[1].rstrip("\n")
                    )
                if "%rwf" in line.lower():
                    self.runvalues["extra_files"].append(
                        line.split("=")[1].rstrip("\n")
                    )
                if "%mem" in line.lower():
                    self.runvalues["memory_in_input"] = True
                    mem_line = line.split("=")[1].rstrip("\n")
                    mem_value, mem_unit = re.match(
                        r"(\d+)([a-zA-Z]+)", mem_line
                    ).groups()
                    if mem_unit.upper() == "GB":
                        self.runvalues["gaussian_memory"] = int(mem_value) * 1024
                    elif mem_unit.upper() == "GW":
                        self.runvalues["gaussian_memory"] = int(mem_value) * 1024 / 8
                    elif mem_unit.upper() == "MB":
                        self.runvalues["gaussian_memory"] = int(mem_value)
                    elif mem_unit.upper() == "MW":
                        self.runvalues["gaussian_memory"] = int(mem_value) / 8
                if "nbo6" in line.lower() or "npa6" in line.lower():
                    self.runvalues["nbo"] = True
                if "FILE=" in line:
                    # TITLE=FILENAME
                    self.runvalues["nbo_basefilename"] = line.split("=")[1].rstrip(
                        " \n"
                    )

    def get_values_from_orca_input_file(self):
        """Parse Orca input file and retrieve useful information"""
        with open(self.input_file, "r") as file:
            # Go through lines and test if they contain nproc, mem, etc. related
            # directives.
            for line in file.readlines():
                if "pal" in line.lower():
                    # Line such as "! Opt PAL12 Freq"
                    extract = re.search(r"pal([0-9]+)", line, flags=re.IGNORECASE)
                    if extract:
                        self.runvalues["nproc_in_input"] = True
                        self.runvalues["cores"] = int(extract.group()[3:])
                if "nprocs" in line.lower():
                    # Line is %pal nprocs 12 end, with possible line breaks before nprocs and after 12.
                    self.runvalues["nproc_in_input"] = True
                    self.runvalues["cores"] = int(
                        re.search(r"nprocs\s+([0-9]+)", line).group().split()[1]
                    )
                if "nbo6" in line.lower() or "npa6" in line.lower():
                    self.runvalues["nbo"] = True
                if "FILE=" in line:
                    # FILE=FILENAME
                    self.runvalues["nbo_basefilename"] = line.split("=")[1].rstrip(
                        " \n"
                    )
                if "moinp" in line.lower():
                    # %moinp "filename.gbw"
                    self.runvalues["extra_files"].append(line.split()[-1].strip('"'))
                if "inhessname" in line.lower():
                    # InHessName "FirstJob.hess"
                    self.runvalues["extra_files"].append(line.split()[-1].strip('"'))
                if "NEB_End_XYZFile" in line:
                    # NEB_End_XYZFile "NEB_end_file.xyz"
                    self.runvalues["extra_files"].append(line.split()[1].strip('"'))

    def fill_missing_values(self):
        """Compute and fill all missing values."""
        # Check node number
        if self.runvalues["nodes"] > 1:
            raise ValueError(
                "Multiple nodes not supported on this cluster for gaussian."
            )
        if self.runvalues["nproc_in_input"] and self.runvalues["cores"] > 48:
            raise ValueError("Number of cores cannot exceed 48 for one node.")

        memory, gaussian_memory = self.compute_memory()
        self.runvalues["memory"] = memory
        self.runvalues["gaussian_memory"] = gaussian_memory

        if memory - gaussian_memory < 6000:
            # Too little overhead
            raise ValueError("Too much memory required for Gaussian to run properly")
        if gaussian_memory > 180000:
            # Too much memory
            raise ValueError("Exceeded max allowed memory")

    def create_shlexnames(self):
        """Return dictionary containing shell escaped names for all possible files."""
        shlexnames = dict()
        input_basename = os.path.splitext(self.runvalues["inputfile"])[0]
        shlexnames["inputfile"] = shlex.quote(self.runvalues["inputfile"])
        shlexnames["basename"] = shlex.quote(input_basename)
        return shlexnames

    def fill_from_commandline(self, cmdline_args):
        """Merge command line arguments into runvalues."""
        self.runvalues["inputfile"] = cmdline_args["inputfile"]
        if cmdline_args["nodes"]:
            self.runvalues["nodes"] = cmdline_args["nodes"]
        if cmdline_args["cores"]:
            self.runvalues["cores"] = cmdline_args["cores"]
        if cmdline_args["walltime"]:
            self.runvalues["walltime"] = cmdline_args["walltime"]
        if cmdline_args["memory"]:
            self.runvalues["memory"] = cmdline_args["memory"]

    def compute_memory(self):
        """
        Return ideal memory value for Irene.

        3.75GB per core, 180GB max, Remove 6GB for system.
        """
        if self.runvalues["memory_in_input"]:
            # Memory defined in input file
            gaussian_memory = self.runvalues["gaussian_memory"]
            if self.runvalues["cores"]:
                # Cores also defined in input file or from command_line: We adjust the number of core requirement
                # so that the memory required is allocated
                if gaussian_memory / self.runvalues["cores"] > 3750:
                    self.runvalues["cores"] = ceil(float(gaussian_memory) / 3750)
                    memory = 3750 * self.runvalues["cores"]
                else:
                    memory = 3750 * self.runvalues["cores"]
            else:
                memory = gaussian_memory + 6000
        else:
            # Memory not defined in input
            if self.runvalues["cores"]:
                # NProc defined in input or from command line
                # Give 3.75 GB per core, remove overhead, min of 2GB if one core.
                memory = 3750 * self.runvalues["cores"]
                gaussian_memory = max(memory - 6000, 2000)
            else:
                # All memory is available, give Gaussian 170GB to run.
                memory = 180000
                gaussian_memory = 170000

        return memory, gaussian_memory

    def walltime_as_list(self):
        """Return walltime as list: [20,00,00]"""
        return [int(x) for x in self.runvalues["walltime"].split(":")]

    def create_run_file(self, output):
        """
        Create .sh file that contains the script to actually run on the server.

        Structure:
            - SBATCH instructions for the queue manager
            - setup software on the nodes
            - creation of scratch, copy necessary files
            - Run calculation
            - Copy appropriate files back to $HOME
            - Cleanup scratch
        """
        # Setup logging
        logger = logging.getLogger()

        # Setup names to use in file
        logger.debug("Runvalues:  %s", self.runvalues)
        logger.debug("Shlexnames: %s", self.shlexnames)

        out = [
            "#!/bin/bash\n",
            "#MSUB -q skylake\n",  # Update if partition changes
            "#MSUB -A gen6494\n",  # To update with account name if it changes.
            "#MSUB -J " + self.shlexnames["inputfile"] + "\n",
            "#MSUB -N 1\n",
            "#MSUB -@ user@server.org:begin,end\n",
        ]
        if self.__software == "g16" and not (self.runvalues["nbo"]):  # Gaussian 16 but no NBO required
            out.extend(["#MSUB -m scratch\n"])
        else:
            out.extend("#MSUB -m scratch,work\n", )
        if self.runvalues["cores"]:  # e.g. is not None
            out.extend(["#MSUB -n " + str(self.runvalues["cores"]) + "\n"])
        else:
            out.extend(["#MSUB -n 48\n"])

        walltime_in_seconds = self.walltime_as_list()
        walltime_in_seconds = (
            3600 * walltime_in_seconds[0]
            + 60 * walltime_in_seconds[1]
            + walltime_in_seconds[2]
        )

        out.extend(
            [
                "#MSUB -T " + str(walltime_in_seconds) + "\n",
                "#MSUB -e " + self.shlexnames["basename"] + ".slurmerr\n",
                "#MSUB -o " + self.shlexnames["basename"] + ".slurmout\n",
                "\n",
                "set +x\n",
            ]
        )

        if not self.runvalues[
            "cores"
        ]:  # Number of cores not set from either command line or input file
            out.extend(
                [
                    "# Compute actual cpu number\n",
                    "NCPU=$(lscpu -p | egrep -v '^#' | sort -u -t, -k 2,4 | wc -l)\n\n",
                ]
            )
        else:
            out.extend(
                ["# Set NCPU value\n", "NCPU=" + str(self.runvalues["cores"]) + "\n\n"]
            )
        out.extend(
            [
                "# Load Modules\n",
                "module purge\n",
                "module switch dfldatadir/gen6494\n",
            ]
        )
        if self.__software == "g16":
            out.extend(
                [
                    "module load gaussian/16-C.01\n",
                    "\n",
                    "# Setup Gaussian specific variables\n",
                    ". $GAUSSIAN_ROOT/g16/bsd/g16.profile\n",
                    "\n",
                ]
            )
        elif self.__software == "orca":
            out.extend(
                [
                    "module load mpi/openmpi/4.1.1\n",
                    "\n",
                    "# Setup Orca specific variables\n",
                    "export ORCA_BIN_DIR=$GEN6494_ALL_CCCWORKDIR/orca\n",
                    "export PATH=$PATH:$ORCA_BIN_DIR\n",
                    "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORCA_BIN_DIR\n",
                    "\n",
                ]
            )
        if self.runvalues["cores"]:
            out.extend(
                ["export OMP_NUM_THREADS=", str(self.runvalues["cores"]), "\n", "\n"]
            )
        else:
            out.extend(["export OMP_NUM_THREADS=$NCPU\n", "\n"])

        # Manage NBO settings
        if self.runvalues["nbo"]:
            out.extend(
                [
                    "# Setup NBO6\n",
                    "export NBOBIN=$GEN6494_ALL_CCCWORKDIR/nbo6/bin\n",
                    "export PATH=$PATH:$NBOBIN\n",
                    "\n",
                ]
            )

        # Setup scratch
        out.extend(
            [
                "# Setup Scratch\n",
                "export SCRATCHDIR=$CCCSCRATCHDIR/$BRIDGE_MSUB_JOBID\n",
                "mkdir -p $SCRATCHDIR\n",
                "\n",
                "# Copy input file\n",
                "cp -f " + self.shlexnames["inputfile"] + " $SCRATCHDIR\n\n",
            ]
        )
        # For all files in "extra_files", check if they exist and copy them to Scratch if they do.
        for file in self.runvalues["extra_files"]:
            out.extend(
                [
                    "if [ -f " + file + " ] \n",
                    "then\n",
                    "  cp " + file + " $SCRATCHDIR\n",
                    "fi\n\n",
                ]
            )
        out.extend(
            [
                "cd $SCRATCHDIR\n",
                "\n",
                "# Print job info in output file\n",
                'echo "job_id : $BRIDGE_MSUB_JOBID"\n',
                'echo "job_name : $BRIDGE_MSUB_REQNAME"\n',
                'echo "$BRIDGE_MSUB_NPROC processes"\n',
                "\n",
            ]
        )

        # Build and add Gaussian starting line
        out.extend(["# Start " + self.__software + "\n"])
        if self.__software == "g16":
            out.extend(self.gaussian_start_line())
        elif self.__software == "orca":
            if not (self.runvalues["nproc_in_input"]):
                out.extend(
                    [
                        "# Add nprocs directive to header of "
                        + self.shlexnames["inputfile"]
                        + "\n",
                        "sed -i '1s;^;%pal\\n  nprocs '$NCPU'\\nend\\n\\n;' "
                        + self.shlexnames["inputfile"]
                        + "\n",
                        "\n",
                    ]
                )
            out.extend(self.orca_start_line())

        out.append("# Move files back to original directory\n")

        if self.__software == "g16":
            out.extend(
                [
                    "cp " + self.shlexnames["basename"] + ".log $BRIDGE_MSUB_PWD\n",
                    "\n",
                    "# If chk file exists, create fchk and copy everything\n",
                    "for f in $SCRATCHDIR/*.chk; do\n",
                    '    [ -f "$f" ] && formchk $f\n',
                    "done\n",
                    "\n",
                    "for f in $SCRATCHDIR/*chk; do\n",
                    '    [ -f "$f" ] && cp $f $BRIDGE_MSUB_PWD\n',
                    "done\n",
                    "\n",
                    "# If Gaussian crashed or was stopped somehow, copy the rwf\n",
                    "for f in $SCRATCHDIR/*rwf; do\n",
                    "    mkdir -p $CCCSCRATCHDIR/rwf\n"
                    # Move rwf as JobName_123456.rwf to the rwf folder in scratch
                    '    [ -f "$f" ] && mv $f $CCCSCRATCHDIR/rwf/'
                    + self.shlexnames["basename"]
                    + "_$BRIDGE_MSUB_JOBID.rwf\n",
                    "done\n",
                ]
            )
        elif self.__software == "orca":
            out.extend(
                [
                    "cp $SCRATCHDIR/*.out $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.gbw $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.engrad $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.xyz $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.loc $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.qro $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.uno $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.unso $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.uco $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.hess $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.cis $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.dat $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.mp2nat $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.nat $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.scfp_fod $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.scfp $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.scfr $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*.nbo $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/FILE.47 $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*_property.txt $BRIDGE_MSUB_PWD\n",
                    "cp $SCRATCHDIR/*spin* $BRIDGE_MSUB_PWD\n",
                    "\n",
                ]
            )
        if self.runvalues["nbo"]:
            out.extend(
                [
                    "# Retrieve NBO Files\n",
                    "cp " + self.runvalues["nbo_basefilename"] + ".*"
                    " $BRIDGE_MSUB_PWD\n"
                    "\n",
                ]
            )
        out.extend(
            [
                "\n",
                "# Empty Scratch directory\n",
                "rm -rf $SCRATCHDIR\n",
                "\n",
                'echo "Computation finished."\n',
                "\n",
            ]
        )

        # Write .sh file
        with open(output, "w") as script_file:
            script_file.writelines(out)

    def gaussian_start_line(self):
        """Start line builder"""

        # Create timeout line
        walltime = self.walltime_as_list()
        runtime = 3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60
        start_line = "timeout " + str(runtime) + " "

        # g09 or g16
        start_line += self.__software + " "

        # Manage processors
        if not self.runvalues["nproc_in_input"]:
            # nproc line not in input, set proc number as a command-line argument
            start_line += '-c="0-$(($NCPU-1))" '
        if not self.runvalues["memory_in_input"]:
            # memory line not in input, set it as command-line argument
            start_line += "-m=" + str(self.runvalues["gaussian_memory"]) + "MB "

        # Add input file
        start_line += "< " + self.shlexnames["inputfile"]

        # Add output file
        start_line += " > " + self.shlexnames["basename"] + ".log\n"

        return start_line

    def orca_start_line(self):
        """Start line builder"""
        # Create timeout line
        walltime = self.walltime_as_list()
        runtime = 3600 * walltime[0] + 60 * walltime[1] + walltime[2] - 60
        start_line = "timeout " + str(runtime) + " "
        # Orca location
        start_line += "$ORCA_BIN_DIR/orca "
        # Add input file
        start_line += self.shlexnames["inputfile"]
        # Add output file
        start_line += " > " + self.shlexnames["basename"] + ".out\n"

        return start_line
