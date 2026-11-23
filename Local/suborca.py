#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path


def check_file_exists(filename):
    """Check if a file already exists."""
    return os.path.exists(filename)


def create_sh_file(inp_filename):
    """Create the SLURM submission script (.sh)."""
    base_name = Path(inp_filename).stem
    sh_filename = f"{base_name}.sh"

    if check_file_exists(sh_filename):
        print(f"⚠️ File {sh_filename} already exists. Stopping execution.")
        sys.exit(1)

    with open(sh_filename, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH --cpus-per-task=1\n")
        f.write("#SBATCH --ntasks=32\n\n")
        f.write("export ORCA_BIN_DIR=/opt/orca_6_1_1_avx2\n")
        f.write("export RSH_COMMAND=ssh\n\n")
        f.write("# Disable useless mechanisms that are throwing errors\n")
        f.write("export HWLOC_COMPONENTS=-gl\n")
        f.write("export OMPI_MCA_btl_vader_single_copy_mechanism=none\n\n")
        f.write("# Start calculation\n")
        f.write(f"FILENAME={base_name}\n\n")
        f.write("$ORCA_BIN_DIR/orca $FILENAME.inp > $FILENAME.out\n")

    # Make the file executable
    os.chmod(sh_filename, 0o755)
    print(f"✅ File {sh_filename} created successfully.")
    return sh_filename


def submit_to_slurm(sh_filename):
    """Submit the job to SLURM automatically."""
    try:
        subprocess.run(["sbatch", sh_filename], check=True)
        print(f"✅ Job submitted to SLURM with command: sbatch {sh_filename}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error submitting to SLURM: {e}")
    except FileNotFoundError:
        print("❌ 'sbatch' command not found. Ensure SLURM is installed.")


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Create a SLURM submission script from an ORCA input file and submit it to SLURM.")
    parser.add_argument("inp_file", type=str, help="Path to the input ORCA file (e.g., CO.inp).")

    # Parse arguments
    args = parser.parse_args()

    # Check if the .inp file exists
    if not check_file_exists(args.inp_file):
        print(f"❌ File {args.inp_file} does not exist.")
        sys.exit(1)

    # Create .sh file
    sh_filename = create_sh_file(args.inp_file)

    # Automatically submit to SLURM
    submit_to_slurm(sh_filename)


if __name__ == "__main__":
    main()
