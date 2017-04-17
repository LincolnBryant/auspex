# AUSPEX
Divining the future by watching birds (among other batch systems)

## Introduction
Auspex is a batch system information library that tries to provide generic 
interfaces for determining various characteristics of a job slot including:
- CPU
- Memory
- Job queue
- Max wall time
- Max disk utilization 
for HTCondor, PBS/Torque, PBS Pro, and SLURM.

It can be run as a standalone script or loaded in as a module.

It should work with Python 2.6+ and has no dependencies outside of the standard
library.
