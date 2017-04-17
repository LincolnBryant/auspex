#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess
import textwrap
import re

class BatchSystem(object):
        """
        This class has a bunch of internal functions that determine the
        batch system type, read the appropriate files or run the appropriate
        programs to determine information about the currently running job's 
        environment. 

        We (at the expense of some power and flexibility) provide a generic
        overview of the job's alotted memory, cpu, disk etc.
        
        """

        def __init__(self):
            self.scheduler = None
            self.memory = None
            self.cpus = None
            self.disk = None
            self.queue = None
            self.walltime = None

            self.determine_scheduler()
            if self.scheduler is 'htcondor':
                self.info_condor()
            elif self.scheduler is 'slurm':
                self.info_slurm()
            elif self.scheduler is 'pbs':
                self.info_pbs()

        def determine_scheduler(self):
            """
            Read the environment to determine the job scheduler running
            by checking for the presence of a job ID or job ad
            """
            job_environment = {
                                "_CONDOR_MACHINE_AD" : "htcondor",
                                "SLURM_JOB_ID" : "slurm",
                                "PBS_JOBID" : "pbs",
                            }
            for key in job_environment:
                if os.environ.get(key) is not None:
                    self.scheduler = job_environment[key]
                    break
                else:
                    self.scheduler = None

        def info_condor(self):
            """
            HTCondor stores the information about a slot in the machine ad.

            This function reads out the machine ad for the following:
                Cpus            --> # of CPUs assigned to this slot
                TotalSlotMemory --> Memory (in MB) for the job slot
                TotalSlotdisk   --> Total amount of disk space (in MB) for this
                                    slot's sandbox

            This function always returns output in base units 
            """
            m_ad = os.environ.get("_CONDOR_MACHINE_AD")
            with open(m_ad) as f:
                classads = dict(classad.split("=",1) for classad in f) 

            try:
                self.cpus = int(classads["Cpus "].strip())
            except:
                self.cpus = None
            
            try:
                self.memory = int(classads["TotalSlotMemory "].strip()) * 1024 * 1024 
            except:
                self.memory = None
            
            try:
                self.disk = int(float(classads["TotalSlotDisk "].strip())) * 1024 * 1024
            except:
                self.disk = None

        def info_pbs(self):
            """
            PBS Torque and PBS Pro require a call to qstat with the PBS_JOBID to
            get the full job information.

            e.g.,
                $ qstat -f $PBS_JOBID

            We read the following information out of the qstat:
                Resource_List.ncpus    --> # of CPUs assigned to the job
                Resource_List.mem      --> Memory (with units) for the job slot
                Resource_List.walltime --> Maximum walltime for the job
                queue                  --> Job queue
            """

            jid = os.environ.get("PBS_JOBID")
            p = subprocess.Popen(["qstat","-f",jid], stdout=subprocess.PIPE)
            out, err = p.communicate()

            # whew lads this is janky
            filtered = []

            for line in out.split('\n'):
                if '=' in line:
                    filtered.append(textwrap.dedent(line))
            kv = dict(key.split("=",1) for key in filtered[1:])
            
            try:
                self.memory = self.memory_parse(kv["Resource_List.mem "].strip())
            except:
                self.memory = None
                            
            """ walltime parsing """
            walltime = kv["Resource_List.walltime "]
            try:
                self.walltime = self.time_convert(walltime) 
            except:
                try:
                    self.walltime = int(os.environ.get("PBS_WALLTIME"))
                except: 
                    self.walltime = None


            """ CPU parsing """
            # sometimes ncpus is available, failing that we use PBS_NP
            # to make our best guess
            try:
                self.cpus = int(kv["Resource_List.ncpus "]) 
            except:
                try: 
                    self.cpus = int(os.environ.get("PBS_NP"))
                except:
                    self.cpus = None

            """ Queue """
            try:
                self.queue = kv["queue "].strip()
            except:
                self.queue = None

        def info_slurm(self):
            """
            Slurm does a reasonably good job of populating the environment with
            all of the information needed -- except for, crucially, wall time.

            We read the following environment variables:
                SLURM_JOB_CPUS_PER_NODE --> # of CPUs assigned to the job
                SLURM_MEM_PER_CPU       --> Memory assigned to job (in MB)
                SLURM_JOB_PARTITION     --> Job queue

            For walltime, we use `scontrol` to get detailed job information.
            e.g.,
                $ scontrol show job $SLURM_JOB_ID
            Then parse the date format and calculate:
                EndTime - StartTime     --> Maximum walltime for the job
            """

            jid = os.environ.get("SLURM_JOB_ID")
            p = subprocess.Popen(["scontrol","show","job",jid], stdout=subprocess.PIPE)
            out, err = p.communicate()

            # SLURM prints multiple key=value pairs per line
            # we tokenize this to a list of lists, then flatten it
            #
            # this seems real bad. need a more idiomatic/pythonig way of doing
            # it
            f = []
            for line in out.split('\n'):
                if line is not '':
                    f.append(line.strip().split(' '))
            filtered = sum(f,[])

            kv = dict(key.split("=",1) for key in filtered)
            
            timelimit = kv["TimeLimit"]
            if timelimit is not "UNLIMITED":
                try:
                    self.walltime = self.time_convert(timelimit) 
                except:
                    self.walltime = None
            else:
                self.wall_seconds = None # if unlimited, its none. 
                                         # if we cant find it, it's none.
                

            # Try to get the info from scontrol, if we fail fall back to
            # using keys from the environment

            try:
                cpus = int(kv["NumCPUs"])
            except:
                    try:
                        cpus = os.environ.get("SLURM_TASKS_PER_NODE")
                    except:
                        cpus = None 
            self.cpus = cpus

            try:
                queue = kv["Partition"]
            except:
                    try:
                        queue = os.environ.get("SLURM_JOB_PARTITION")
                    except:
                        queue = None
            self.queue = queue
            
            try:
                mem = self.memory_parse(kv["MinMemoryCPU"])
            except:
                    try:
                        mem = int(os.environ.get("SLURM_MEM_PER_CPU")) * 1024 * 1024 
                    except:
                        mem = None
            self.memory = mem


        def time_convert(self, timestamp):
            """ Helper function for converting HH:MM:SS to just seconds """
            h,m,s = re.split(':',timestamp)
            return int(h)*3600 + int(m)*60 + int(s)

        def memory_parse(self, memory):
            """ 
            Schedulers often attach units to the memory values, so we gotta
            split those out and multiply by the right values
            """
                 
            p = re.compile('(\d+)\s*(\w+)')
            mem = memory.strip().upper()

            parsed_mem = p.match(mem).groups()

            if 'T' in parsed_mem[1]:
                mem_bytes = int(parsed_mem[0]) * 1024 * 1024 * 1024 * 1024
            if 'G' in parsed_mem[1]:
                mem_bytes = int(parsed_mem[0]) * 1024 * 1024 * 1024
            if 'M' in parsed_mem[1]:
                mem_bytes = int(parsed_mem[0]) * 1024 * 1024

            return mem_bytes

if __name__ == "__main__":
    bs = BatchSystem()
    if bs.scheduler is None:
        print("Cannot determine scheduler")
        sys.exit(1)

    print("job.mem_bytes=%s" % bs.memory)
    print("job.num_cpus=%s" % bs.cpus)
    print("job.disk_bytes=%s" % bs.disk)
    print("job.queue=%s" % bs.queue)
    print("job.wall_seconds=%s" % bs.walltime)
