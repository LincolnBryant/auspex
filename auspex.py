#!/usr/bin/env python
from __future__ import print_function
import os
import sys


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

        def determine_scheduler(self):
            """
            Read the environment to determine the job scheduler running
            by checking for the presence of a job ID or job ad
            """
            job_environment = ["_CONDOR_JOB_AD", "SLURM_JOB_ID", "PBS_JOBID"]
            for key in job_environment:
                if os.environ.get(key) is not None:
                    self.scheduler = key
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
            """
            

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
                $ scontrol show job $SLURM_JOBID
            Then parse the date format and calculate:
                EndTime - StartTime     --> Maximum walltime for the job
            """


if __name__ == "__main__":
    bs = BatchSystem()
    if bs.scheduler is None:
        print("Cannot determine scheduler")
        sys.exit(1)

