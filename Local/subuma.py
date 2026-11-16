#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path


def check_file_exists(filename):
    """Check if a file already exists."""
    return os.path.exists(filename)


def create_inp_file(xyz_filename, charge=0, multiplicity=1, solvent=None):
    """Create the ORCA input file (.inp)."""
    base_name = Path(xyz_filename).stem
    inp_filename = f"{base_name}.inp"

    if check_file_exists(inp_filename):
        print(f"⚠️ File {inp_filename} already exists. Stopping execution.")
        sys.exit(1)

    with open(inp_filename, "w") as f:
        f.write("! ExtOpt Opt\n\n")
        f.write("%pal nprocs 25 end\n\n")
        f.write("%maxcore 4000\n\n")
        f.write("%method\n")
        f.write('ProgExt "/opt/orca_external_tools/orca_external_tools/bin/oet_client"\n')

        # Add solvent parameter only if solvent is specified
        if solvent:
            f.write(f'Ext_Params "-b 127.0.0.1:8888 --solvent {solvent} -d cuda"\n')
        else:
            f.write('Ext_Params "-b 127.0.0.1:8888 -d cuda"\n')

        f.write("end\n\n")
        f.write(f"*xyzfile {charge} {multiplicity} {xyz_filename}\n")

    print(f"✅ File {inp_filename} created successfully.")
    return inp_filename


def create_sh_file(xyz_filename):
    """Create the SLURM submission script (.sh)."""
    base_name = Path(xyz_filename).stem
    sh_filename = f"{base_name}.sh"

    if check_file_exists(sh_filename):
        print(f"⚠️ File {sh_filename} already exists. Stopping execution.")
        sys.exit(1)

    with open(sh_filename, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH --cpus-per-task=1\n")
        f.write("#SBATCH --ntasks=32\n\n")
        f.write("export ORCA_BIN_DIR=/opt/orca_6_1_1_avx2\n\n")
        f.write("# Disable useless mechanisms that are throwing errors\n")
        f.write("export HWLOC_COMPONENTS=-gl\n")
        f.write("export OMPI_MCA_btl_vader_single_copy_mechanism=none\n\n")
        f.write(f"FILENAME={base_name}\n\n")
        f.write("source /opt/orca_external_tools/orca_external_tools/bin/activate\n\n")
        f.write('UMA_SERVER_SCRIPT="/opt/orca_external_tools/orca_external_tools/bin/oet_server"\n\n')
        f.write("# Check if UMA solvated server script exists and is executable\n")
        f.write('if [[ ! -x "$UMA_SERVER_SCRIPT" ]]; then\n')
        f.write('    echo "Error: UMA server script not found or not executable: $UMA_SERVER_SCRIPT"\n')
        f.write("    exit 1\n")
        f.write("fi\n\n")
        f.write("# Start the UMA server in the background with nohup and redirect output\n")
        f.write('nohup "$UMA_SERVER_SCRIPT" uma_solvated -b 127.0.0.1:8888 --nthreads 25 > umaserver.log 2>&1 &\n')
        f.write("SERVER_PID=$!\n\n")
        f.write("# Ensure server is killed on script exit\n")
        f.write('trap "kill -9 $SERVER_PID 2>/dev/null" EXIT\n\n')
        f.write("# Wait a bit to ensure the server starts before ORCA begins\n")
        f.write("sleep 5\n\n")
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
    if len(sys.argv) < 2:
        print("Usage: python create_orca_input.py <file.xyz> [charge] [multiplicity] [solvent]")
        print("Example: python create_orca_input.py 3a.xyz 0 1 benzene")
        print("Note: If solvent is not specified, it defaults to None (gas phase).")
        sys.exit(1)

    xyz_filename = sys.argv[1]

    # Check if the .xyz file exists
    if not check_file_exists(xyz_filename):
        print(f"❌ File {xyz_filename} does not exist.")
        sys.exit(1)

    # Get charge, multiplicity, and solvent (default: 0, 1, None)
    charge = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    multiplicity = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    solvent = sys.argv[4] if len(sys.argv) > 4 else None

    # Create .inp and .sh files
    create_inp_file(xyz_filename, charge, multiplicity, solvent)
    sh_filename = create_sh_file(xyz_filename)

    # Automatically submit to SLURM
    submit_to_slurm(sh_filename)


if __name__ == "__main__":
    main()
