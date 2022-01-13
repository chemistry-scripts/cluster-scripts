#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Helper functions to set up theoretical chemistry computations on computing clusters.

Last Update 2020-03-17 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import os
import shlex
import logging
import re


class Computation:
    """
    Class representing the computation that will be ran.
    """

    def __init__(self, input_file, software, cmdline_args):
        """
        Class Builder
        """
        self.__input_file = input_file
        self.__software = software
        self.__runvalues = self.default_run_values()
        self.fill_from_commandline(cmdline_args)
        self.fill_missing_values()
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
        runvalues["walltime"] = "20:00:00"
        runvalues["memory"] = 4000  # In MB
        runvalues["gaussian_memory"] = 1000  # in MB
        runvalues["chk"] = set()
        runvalues["oldchk"] = set()
        runvalues["rwf"] = set()
        runvalues["nproc_in_input"] = False
        runvalues["memory_in_input"] = False
        runvalues["nbo"] = False
        runvalues["nbo_basefilename"] = ""
        runvalues["cluster_section"] = "HSW24"
        return runvalues

    def get_values_from_gaussian_input_file(self):
        """Get core/memory values from input file, reading the Mem and NProcShared parameters."""
        with open(self.input_file, "r") as file:
            # Go through lines and test if they are containing nproc, mem, etc. related
            # directives.
            for line in file.readlines():
                if "%nproc" in line.lower():
                    self.runvalues["nproc_in_input"] = True
                    self.runvalues["cores"] = int(line.split("=")[1].rstrip("\n"))
                if "%chk" in line.lower():
                    self.runvalues["chk"].add(line.split("=")[1].rstrip("\n"))
                if "%oldchk" in line.lower():
                    self.runvalues["oldchk"].add(line.split("=")[1].rstrip("\n"))
                if "%rwf" in line.lower():
                    self.runvalues["rwf"].add(line.split("=")[1].rstrip("\n"))
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

    def fill_missing_values(self):
        """Compute and fill all missing values."""
        # Setup cluster_section according to number of cores
        if not self.runvalues["nproc_in_input"]:
            self.runvalues["cluster_section"] = "HSW24|BDW28"
        elif self.runvalues["cores"] <= 24:
            self.runvalues["cluster_section"] = "HSW24"
        elif self.runvalues["cores"] <= 28:
            self.runvalues["cluster_section"] = "BDW28"
        elif self.runvalues["cores"] > 28 and self.runvalues["nodes"] == 1:
            raise ValueError("Number of cores cannot exceed 28 for one node.")
        elif self.runvalues["nodes"] > 1:
            raise ValueError(
                "Multiple nodes not supported on this cluster for gaussian."
            )

        memory, gaussian_memory = self.compute_memory()
        self.runvalues["memory"] = memory
        self.runvalues["gaussian_memory"] = gaussian_memory

        if memory - gaussian_memory < 6000:
            # Too little overhead
            raise ValueError("Too much memory required for Gaussian to run properly")
        if gaussian_memory > 160000:
            # Too much memory
            raise ValueError("Exceeded max allowed memory")

    def create_shlexnames(self):
        """Return dictionary containing shell escaped names for all possible files."""
        shlexnames = dict()
        input_basename = os.path.splitext(self.runvalues["inputfile"])[0]
        shlexnames["inputfile"] = shlex.quote(self.runvalues["inputfile"])
        shlexnames["basename"] = shlex.quote(input_basename)
        if self.runvalues["chk"] is not None:
            shlexnames["chk"] = [shlex.quote(chk) for chk in self.runvalues["chk"]]
        if self.runvalues["oldchk"] is not None:
            shlexnames["oldchk"] = [
                shlex.quote(oldchk) for oldchk in self.runvalues["oldchk"]
            ]
        if self.runvalues["rwf"] is not None:
            shlexnames["rwf"] = [shlex.quote(rwf) for rwf in self.runvalues["rwf"]]
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
        Return ideal memory value for Jean Zay.

        160GB available per core : remove 6 Gb for overhead.
        """
        memory = 160000
        gaussian_memory = 0

        if self.runvalues["memory_in_input"]:
            # Memory defined in input file
            gaussian_memory = self.runvalues["gaussian_memory"]
        else:
            gaussian_memory = 140000

        return memory, gaussian_memory

    def walltime_as_list(self):
        """Return walltime as list: [20,00,00]"""
        return [int(x) for x in self.runvalues["walltime"].split(":")]

    def create_run_file(self, output):
        """
        Create .sh file that contains the script to actually run on the server.

        Structure:
            - SBATCH instructions for the queue manager
            - setup of Gaussian16 on the nodes
            - creation of scratch, copy necessary files
            - Run Gaussian16
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
            "#MSUB -q skylake\n",  # TODO: Remove queue selection after January 26th
            "#MSUB -A gen12981\n",  # To update with account name if it changes.
            "#MSUB -J " + self.shlexnames["inputfile"] + "\n",
            "#MSUB -N 48\n",
            "#MSUB -m scratch,work\n",
            "#MSUB -@ user@server.org:begin,end\n",
        ]
        if self.runvalues["nproc_in_input"]:
            out.extend(["#MSUB -n=" + str(self.runvalues["cores"]) + "\n"])
        else:
            out.extend(["#MSUB -n=24\n"])

        walltime_in_seconds = self.walltime_as_list()
        walltime_in_seconds = (
            3600 * walltime_in_seconds[0]
            + 60 * walltime_in_seconds[1]
            + walltime_in_seconds[2]
        )

        out.extend(
            [
                "#MSUB --mem=" + str(self.runvalues["memory"]) + "\n",
                "#MSUB -T " + str(walltime_in_seconds) + "\n",
                "#MSUB -e " + self.shlexnames["basename"] + ".slurmerr\n",
                "#MSUB -o " + self.shlexnames["basename"] + ".slurmout\n",
                # TODO: Merge out and err?
                "\n",
            ]
        )

        if not self.runvalues["nproc_in_input"]:  # nproc line not in input
            out.extend(
                [
                    "# Compute actual cpu number\n",
                    "NCPU=$(lscpu -p | egrep -v '^#' | sort -u -t, -k 2,4 | wc -l)\n\n",
                ]
            )
        if self.__software == "g16":
            out.extend(
                [
                    "# Load Gaussian Module\n",
                    "module purge\n",
                    "module switch dfldatadir/gen12981\n",
                    "module load gaussian/16-C.01\n",
                    "\n",
                    "# Setup Gaussian specific variables\n",
                    ". $GAUSSIAN_ROOT/g16/bsd/g16.profile\n",
                ]
            )
        # if self.runvalues["nproc_in_input"]:
        #     out.extend(["export OMP_NUM_THREADS=$SLURM_JOB_CPUS_PER_NODE\n", "\n"])
        # else:
        #     out.extend(["export OMP_NUM_THREADS=$NCPU\n", "\n"])
        # if self.runvalues["nbo"]:
        #     out.extend(
        #         [
        #             "# Setup NBO6\n",
        #             "export NBOBIN=$SHAREDHOMEDIR/nbo6/bin\n",
        #             "export PATH=$PATH:$NBOBIN\n",
        #             "\n",
        #         ]
        #     )
        out.extend(
            [
                "# Setup Scratch\n",
                "export GAUSS_SCRDIR=$CCCSCRATCHDIR/gaussian/$BRIDGE_MSUB_JOBID\n",
                "mkdir -p $GAUSS_SCRDIR\n",
                "\n",
                "# Copy input file\n",
                "cp -f " + self.shlexnames["inputfile"] + " $GAUSS_SCRDIR\n\n",
            ]
        )
        # If chk file is defined in input and exists, copy it in scratch
        if self.runvalues["chk"] != set():
            out.extend("# Copy chk file in scratch if it exists\n")
            for chk in self.shlexnames["chk"]:
                out.extend(
                    [
                        "if [ -f " + chk + " ] \n",
                        "then\n",
                        "  cp " + chk + " $GAUSS_SCRDIR\n",
                        "fi\n\n",
                    ]
                )
        # If oldchk file is defined in input and exists, copy it in scratch
        if self.runvalues["oldchk"] != set():
            out.extend("# Copy oldchk file in scratch if it exists\n")
            for oldchk in self.shlexnames["oldchk"]:
                out.extend(
                    [
                        "if [ -f " + oldchk + " ] \n",
                        "then\n",
                        "  cp " + oldchk + " $GAUSS_SCRDIR\n",
                        "fi\n\n",
                    ]
                )
        # If rwf file is defined in input and exists, copy it in scratch
        if self.runvalues["rwf"] != set():
            out.extend("# Copy rwf file in scratch if it exists\n")
            for rwf in self.shlexnames["rwf"]:
                out.extend(
                    [
                        "if [ -f " + rwf + " ] \n",
                        "then\n",
                        "  cp " + rwf + " $GAUSS_SCRDIR\n",
                        "fi\n\n",
                    ]
                )
        out.extend(
            [
                "cd $GAUSS_SCRDIR\n",
                "\n",
                "# Print job info in output file\n",
                'echo "job_id : $BRIDGE_MSUB_JOBID"\n',
                # 'echo "job_name : $SLURM_JOB_NAME"\n',
                # 'echo "node_number : $SLURM_JOB_NUM_NODES nodes"\n',
                # 'echo "core number : $SLURM_JOB_CPUS_PER_NODE cores"\n',
                # 'echo "Node list : $SLURM_JOB_NODELIST"\n',
                "\n",
            ]
        )

        # Build and add Gaussian starting line
        out.extend(["# Start Gaussian\n"])
        gaussian_start_line = self.gaussian_start_line()
        out.extend(gaussian_start_line)

        out.extend(
            [
                "# Move files back to original directory\n",
                "cp " + self.shlexnames["basename"] + ".log $SLURM_SUBMIT_DIR\n",
                "\n",
            ]
        )
        out.extend(
            [
                "# If chk file exists, create fchk and copy everything\n",
                "for f in $GAUSS_SCRDIR/*.chk; do\n",
                '    [ -f "$f" ] && formchk $f\n',
                "done\n",
                "\n",
                "for f in $GAUSS_SCRDIR/*chk; do\n",
                '    [ -f "$f" ] && cp $f $SLURM_SUBMIT_DIR\n',
                "done\n",
                "\n",
            ]
        )
        if self.runvalues["nbo"]:
            out.extend(
                [
                    "# Retrieve NBO Files\n",
                    "cp " + self.runvalues["nbo_basefilename"] + ".*"
                    " $SLURM_SUBMIT_DIR\n"
                    "\n",
                ]
            )
        out.extend(
            [
                "# If Gaussian crashed or was stopped somehow, copy the rwf\n",
                "for f in $GAUSS_SCRDIR/*rwf; do\n",
                "    mkdir -p $CCCSCRATCHDIR/gaussian/rwf\n"
                # Move rwf as JobName_123456.rwf to the rwf folder in scratch
                '    [ -f "$f" ] && mv $f $SCRATCH/gaussian/rwf/'
                + self.shlexnames["basename"]
                + "_$SLURM_JOB_ID.rwf\n",
                "done\n",
                "\n",
                "# Empty Scratch directory\n",
                "rm -rf $GAUSS_SCRDIR\n",
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
            # nproc line not in input, set proc number as command-line argument
            start_line += '-c="0-$(($NCPU-1))" '
        if not self.runvalues["memory_in_input"]:
            # memory line not in input, set it as command-line argument
            start_line += "-m=" + str(self.runvalues["gaussian_memory"]) + "MB "

        # Add input file
        start_line += "< " + self.shlexnames["inputfile"]

        # Add output file
        start_line += " >" + self.shlexnames["basename"] + ".log\n"

        return start_line
