#!/usr/bin/python3
import re
import sys


"""When using less than all nodes, you need to feed the ids of cpus to gaussian"""
with open("/proc/self/status") as cpu_status:
    for line in cpu_status:
        if re.search("Cpus_allowed_list", line):
            cpus = line  # Cpus_allowed_list:      0-1,24-31,128-129,152-159
            break
cpu_list = cpus.split(":")[-1].strip().split(",")  # ['0-1','24-31','128-129',''152-159']

sys.stdout.write(",".join(cpu_list[:len(cpu_list)//2])) # 0-1,24-31
