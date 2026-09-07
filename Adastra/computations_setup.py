#!/usr/bin/env python3

"""
Helper functions to set up theoretical chemistry computations on computing clusters.

Last Update 2026-05-21 by Emmanuel Nicolas
email emmanuel.nicolas -at- cea.fr
Requires Python3 to be installed.
"""

import logging
import os
import re
import shlex


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
        # Orca only here. Switch if you want to add extra software/multiple versions
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
        runvalues["cores"] = "24"
        runvalues["walltime"] = "24:00:00"
        runvalues["memory"] = 4000  # In MB
        runvalues["nproc_in_input"] = False
        runvalues["memory_in_input"] = False
        runvalues["nbo"] = False
        runvalues["nbo_basefilename"] = (
            ""  # TODO Check FILE option in orca --> p.711 within NBOKEYLIST
        )
        runvalues["extra_files"] = []
        return runvalues

    def get_values_from_orca_input_file(self):
        """Parse Orca input file and retrieve useful information"""
        with open(self.input_file, "r") as file:
            # Go through lines and test if they contain nproc, mem, etc. related
            # directives.
            for line in file:
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
                # if ".xyz" in line:
                # There is a line containing an .xyz file name. We collected all names and add them
                # TODO!!!!!
                # self.runvalues["extra_files"].append(line.split()[-1].strip('"'))

    def create_shlexnames(self):
        """Return dictionary containing shell escaped names for all possible files."""
        shlexnames = {}
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
        Return ideal memory value for Adastra

        3.75GB per core, 220GB max, Remove 6GB for system.
        """

        return memory

    def walltime_as_list(self):
        """Return walltime as list: [20,00,00]"""
        return [int(x) for x in self.runvalues["walltime"].split(":")]

    def walltime_in_seconds(self):
        """Return walltime in seconds."""
        walltime_in_seconds = self.walltime_as_list()
        walltime_in_seconds = (
            3600 * walltime_in_seconds[0]
            + 60 * walltime_in_seconds[1]
            + walltime_in_seconds[2]
        )
        return walltime_in_seconds

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
            "#SBATCH --constraint=GENOA\n",  # Update if partition changes
            "#SBATCH --account=cad14129\n",  # To update with account name if it changes.
            "#SBATCH --job-name=" + self.shlexnames["inputfile"] + "\n",
            "#SBATCH --nodes 1\n",
            "#SBATCH --cpus-per-task=1\n",
            "#SBATCH --threads-per-core=1\n",
            "#SBATCH --output=%x.%j.slurmout\n",
            "#SBATCH --error=%x.%j.slurmerr\n",
            "#SBATCH --ntasks " + str(self.runvalues["cores"]) + "\n",
            "#SBATCH --time=" + str(self.walltime_in_seconds()) + "\n",
            # "#SBATCH -@ user@server.org:begin,end\n",  # FIXME Question on this: still sending mail?
            "\n",
        ]

        # Starting modules/loading etc.
        #
        out.extend(
            [
                "# Load Modules\n",
                "module purge 2>/dev/null\n",
                "module load cpe/25.09\n",
                "module load craype-x86-genoa\n",
                "module load PrgEnv-cray\n",
                "module load python/3.12.1\n",
                "module use /lus/work/CT8/cad14129/SHARED/Configuration.spack-user-5.0.0/modules/tcl/linux-rhel9-zen4/gcc/13.2.1/zen4\n",
                "module load openmpi/4.1.8-jcdl\n",
                "\n",
                "export OMP_NUM_THREADS=$SLURM_NTASKS\n",
                "export FI_PROVIDER=shm\n",
                "\n",
                "# Put orca in the path\n",
                "export ORCA_BIN_DIR=$WORK_cad14129_SHARED/orca_6_1_1\n",
                "export PATH=$PATH:$ORCA_BIN_DIR\n",
                "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORCA_BIN_DIR\n",
            ]
        )

        # Manage NBO settings
        if self.runvalues["nbo"]:
            out.extend(
                [
                    "# Setup NBO6\n",
                    "export NBOBIN=$SHAREDWORKDIR/nbo6/bin\n",
                    "export NBOEXE=$NBOBIN/nbo6.i8.exe\n",
                    "export PATH=$PATH:$NBOBIN\n",
                    "\n",
                ]
            )

        # Actual calculation setup
        out.extend(
            [
                "export JOB_NAME=" + self.shlexnames["inputfile"] + "\n",
                "# Setup Scratch\n",
                "export TMP_SCRATCH=$SCRATCHDIR/$SLURM_JOB_ID\n",
                "mkdir -p $TMP_SCRATCH\n",
                "\n",
                "# Copy input file\n",
                "cp -f $JOB_NAME.inp $SCRATCHDIR\n\n",
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
                "cd $TMP_SCRATCH\n",
                "\n",
                "# Print job info in output file\n",  # FIXME is it necessary?
                'echo "job_id : $BRIDGE_MSUB_JOBID"\n',
                'echo "job_name : $BRIDGE_MSUB_REQNAME"\n',
                'echo "$BRIDGE_MSUB_NPROC processes"\n',
                "\n",
            ]
        )

        # Build and add start line
        out.extend(["# Start " + self.__software + "\n"])
        if not (self.runvalues["nproc_in_input"]):
            out.extend(
                [
                    "# Add nprocs directive to header of "
                    + self.shlexnames["inputfile"]
                    + "\n",
                    "sed -i '1s;^;%pal\\n  nprocs '$SLURM_NTASKS'\\nend\\n\\n;' "
                    + self.shlexnames["inputfile"]
                    + "\n",
                    "\n",
                ]
            )
        out.extend(self.orca_start_line())

        out.extend(
            [
                "## --- Wrap up time! ---\n\n",
                "# Move files back to original directory\n",
                "cp $SCRATCHDIR/*.out $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.gbw $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.engrad $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.xyz $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.loc $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.qro $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.uno $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.unso $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.uco $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.hess $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.densities $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.densitiesinfo $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.cis $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.dat $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.mp2nat $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.nat $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.scfp_fod $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.scfp $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.scfr $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*.nbo $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/FILE.47 $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*_property.txt $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "cp $SCRATCHDIR/*spin* $SLURM_SUBMIT_DIR 2>/dev/null\n",
                "\n",
            ]
        )
        if self.runvalues["nbo"]:
            out.extend(
                [
                    "# Retrieve NBO Files\n",
                    "cp "
                    + self.runvalues["nbo_basefilename"]
                    + ".* $SLURM_SUBMIT_DIR 2>/dev/null\n"
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

    def orca_start_line(self):
        """Start line builder"""
        # Create timeout line
        runtime = self.walltime_in_seconds() - 60
        start_line = "timeout " + str(runtime) + " "
        # Orca location
        start_line += "$ORCA_BIN_DIR/orca "
        # Add input file
        start_line += self.shlexnames["inputfile"]
        # Add output file
        start_line += " > " + self.shlexnames["basename"] + ".out\n"

        return start_line
