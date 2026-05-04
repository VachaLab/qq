# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Batch-system support for qq.

This module groups all components that allow qq to interact with HPC batch
schedulers. It defines the abstract interfaces for jobs, queues, and nodes,
together with the concrete backends for PBS, Slurm, and site-specific Slurm
variants.
"""
