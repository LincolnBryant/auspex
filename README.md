# AUSPEX
Divining the future by watching birds (among other batch systems)

## Introduction
Auspex is a batch system information library that tries to provide generic 
interfaces for determining various characteristics of a job slot _in situ_ 

Currently there are generic interfaces for:
- CPU
- Memory
- Job queue
- Max wall time
- Max disk utilization 

for HTCondor, PBS/Torque, PBS Pro, and SLURM.

## Compatibility
It can be run as a standalone script or loaded in as a module.

It should work with Python 2.6+ and has no dependencies outside of the standard
library.

## Usage
Within your Python script, you ought to be able to do something like:
```python
import auspex
bs = auspex.BatchSystem()

print("job.mem_bytes=%s" % bs.memory)
print("job.num_cpus=%s" % bs.cpus)
print("job.disk_bytes=%s" % bs.disk)
print("job.queue=%s" % bs.queue)
print("job.wall_seconds=%s" % bs.walltime)
```

Maybe? Hopefully? Alternatively just run `auspex.py` from a shell to get the default output. 

**NOTE**: If Auspex can't find something, it will simply return None. At best, Auspex only provides _hints_ about the scheduler system running underneath. Absence of evidence is not evidence of absence!


## Examples
Here's an example output for HTCondor using a canned machine ad (included in the samples/ directory)

```bash
$ export _CONDOR_MACHINE_AD=samples/condor-machine-ad
$ ./auspex.py
job.mem_bytes=134217728
job.num_cpus=1
job.disk_bytes=485827280896
job.queue=None
job.wall_seconds=None
```
