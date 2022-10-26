#!/usr/bin/python3
import re
import sys


"""When using less than all nodes, you need to feed the ids of cpus to gaussian"""
with open("/proc/self/status") as cpu_status:
    for line in cpu_status:
        if re.search("Cpus_allowed_list", line):
            cpus = line  # Cpus_allowed_list:      18-23,42-47
            break
cpu_list = cpus.split(":")[-1]  # 18-23,42-47
sys.stdout.write(cpu_list.strip())
