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
from typing import ClassVar


def default_run_values():
    """Return the run values with their default settings."""
    return {
        "inputfile": "",
        "cores": 24,
        "walltime": "24:00:00",
        "memory": 90,  # In GB
        "nproc_in_input": False,
        "nbo": False,
        "nbo_basefilename": "",
        "extra_files": [],
    }


class Computation:
    """Class representing the computation that will be run."""

    # Files to copy back from the scratch directory once the job finishes.
    OUTPUT_GLOBS: ClassVar[list[str]] = [
        "*.out",
        "*.gbw",
        "*.engrad",
        "*.xyz",
        "*.loc",
        "*.qro",
        "*.uno",
        "*.unso",
        "*.uco",
        "*.hess",
        "*.densities",
        "*.densitiesinfo",
        "*.cis",
        "*.dat",
        "*.mp2nat",
        "*.nat",
        "*.scfp_fod",
        "*.scfp",
        "*.scfr",
        "*.nbo",
        "FILE.47",
        "*_property.txt",
        "*spin*",
    ]

    def __init__(self, input_file, software, cmdline_args):
        logger = logging.getLogger()
        self.input_file = input_file
        self.software = software
        self.basename = os.path.splitext(input_file)[0]
        self.shlexnames = {
            "inputfile": shlex.quote(input_file),
            "basename": shlex.quote(self.basename),
        }
        self.runvalues = default_run_values()
        logger.debug("Runvalues default: %s", self.runvalues)
        self.fill_from_commandline(cmdline_args)
        logger.debug("Runvalues cmdline: %s", self.runvalues)
        # Only ORCA is currently supported.
        self.get_values_from_orca_input_file()
        logger.debug("Runvalues orca:    %s", self.runvalues)

    @property
    def walltime(self):
        """Walltime as ``"HH:MM:SS"``."""
        return self.runvalues["walltime"]

    def fill_from_commandline(self, cmdline_args):
        """Merge non-empty command line arguments into the run values."""
        self.runvalues["inputfile"] = cmdline_args["inputfile"]
        self.runvalues["cores"] = cmdline_args.get("cores") or self.runvalues["cores"]
        self.runvalues["walltime"] = (
            cmdline_args.get("walltime") or self.runvalues["walltime"]
        )
        self.runvalues["memory"] = (
            cmdline_args.get("memory") or self.runvalues["memory"]
        )

    def get_values_from_orca_input_file(self):
        """Read cores, NBO settings and extra input files from the ORCA input."""
        with open(self.input_file) as input_file:
            for line in input_file:
                lowered = line.lower()
                # Number of cores, either "! ... PAL12 ..." or "%pal nprocs 12 ..."
                match = re.search(r"pal\s*(\d+)", line, re.IGNORECASE)
                if match:
                    self.runvalues.update(
                        nproc_in_input=True, cores=int(match.group(1))
                    )
                match = re.search(r"nprocs\s+(\d+)", lowered)
                if match:
                    self.runvalues.update(
                        nproc_in_input=True, cores=int(match.group(1))
                    )
                if "nbo6" in lowered or "npa6" in lowered:
                    self.runvalues["nbo"] = True
                if "FILE=" in line:
                    # FILE=FILENAME
                    self.runvalues["nbo_basefilename"] = line.split("=", 1)[1].strip()
                # Extra input files referenced by the calculation.
                if "moinp" in lowered or "inhessname" in lowered:
                    self.runvalues["extra_files"].append(line.split()[-1].strip('"'))
                if "NEB_End_XYZFile" in line:
                    self.runvalues["extra_files"].append(line.split()[1].strip('"'))

    def compute_memory(self):
        """Return the ideal amount of memory in GB for the given number of cores."""
        # 3.75 GB per core leaves some headroom for the system.
        return int(3.75 * int(self.runvalues["cores"]))

    def walltime_in_seconds(self):
        """Return the walltime expressed in seconds."""
        hours, minutes, seconds = (
            int(x) for x in self.runvalues["walltime"].split(":")
        )
        return 3600 * hours + 60 * minutes + seconds

    def create_run_file(self, output):
        """Write the shell script that runs the computation on the cluster."""
        logger = logging.getLogger()
        logger.debug("Runvalues: %s", self.runvalues)

        out = []

        # --- SLURM directives ---
        out.append("#!/bin/bash\n")
        out.append("#SBATCH --constraint=GENOA\n")
        out.append("#SBATCH --account=cad14129\n")
        out.append("#SBATCH --job-name=" + self.shlexnames["basename"] + "\n")
        for option in (
            "--nodes 1",
            "--cpus-per-task=1",
            "--threads-per-core=1",
            "--output=%x.%j.slurmout",
            "--error=%x.%j.slurmerr",
            "--ntasks " + str(self.runvalues["cores"]),
            "--mem=" + str(self.compute_memory()) + "G",
            "--time=" + str(self.walltime_in_seconds() // 60),
        ):
            out.append("#SBATCH " + option + "\n")
        out.append("\n")

        # --- Load modules and set up ORCA ---
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
                "# Put ORCA in the path\n",
                "export ORCA_BIN_DIR=$WORK_cad14129_SHARED/orca_6_1_1\n",
                "export PATH=$PATH:$ORCA_BIN_DIR\n",
                "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORCA_BIN_DIR\n",
            ]
        )

        # --- NBO setup ---
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

        # --- Scratch directory and input files ---
        out.extend(
            [
                "# Setup Scratch\n",
                "export TMP_SCRATCH=$SCRATCHDIR/$SLURM_JOB_ID\n",
                "mkdir -p $TMP_SCRATCH\n",
                "\n",
                "# Copy input file\n",
                "cp -f " + self.shlexnames["inputfile"] + " $TMP_SCRATCH\n\n",
            ]
        )
        for extra_file in self.runvalues["extra_files"]:
            out.extend(
                [
                    f"if [ -f {extra_file} ]\n",
                    "then\n",
                    f"  cp {extra_file} $TMP_SCRATCH\n",
                    "fi\n\n",
                ]
            )
        out.extend(["cd $TMP_SCRATCH\n", "\n"])

        # --- Run the calculation ---
        out.append("# Start " + self.software + "\n")
        if not self.runvalues["nproc_in_input"]:
            out.extend(
                [
                    "# Add nprocs directive to the header of "
                    + self.shlexnames["inputfile"]
                    + "\n",
                    "sed -i '1s;^;%pal\\n  nprocs '$SLURM_NTASKS'\\nend\\n\\n;' "
                    + self.shlexnames["inputfile"]
                    + "\n",
                    "\n",
                ]
            )
        out.append(self.orca_start_line())

        # --- Copy results back and clean up ---
        out.extend(
            [
                "## --- Wrap up time! ---\n\n",
                "# Move files back to the original directory\n",
            ]
        )
        for glob in self.OUTPUT_GLOBS:
            out.append(f"cp $TMP_SCRATCH/{glob} $SLURM_SUBMIT_DIR 2>/dev/null\n")
        if self.runvalues["nbo"]:
            out.extend(
                [
                    "# Retrieve NBO files\n",
                    "cp "
                    + self.runvalues["nbo_basefilename"]
                    + ".* $SLURM_SUBMIT_DIR 2>/dev/null\n\n",
                ]
            )
        out.extend(
            [
                "\n",
                "# Empty the scratch directory\n",
                "rm -rf $TMP_SCRATCH\n",
                "\n",
                'echo "Computation finished."\n',
                "\n",
            ]
        )

        with open(output, "w") as script_file:
            script_file.writelines(out)

    def orca_start_line(self):
        """Return the shell command that launches ORCA with a timeout."""
        runtime = self.walltime_in_seconds() - 60
        return (
            f"timeout {runtime} $ORCA_BIN_DIR/orca "
            f"{self.shlexnames['inputfile']} > {self.shlexnames['basename']}.out\n"
        )
