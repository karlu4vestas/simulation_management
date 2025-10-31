"""
Clean Agent module for VTS simulation cleanup operations.

This module provides a multi-threaded implementation for cleaning
VTS simulations, separated from the legacy clean_old implementation.

Main entry point:
    clean_main() - Process and clean simulations

Key components:
    - CleanMode - ANALYSE or DELETE mode
    - CleanProgressReporter - Abstract base for progress reporting
    - CleanProgressWriter - Default progress reporter implementation
    - Simulation - Stub class for simulation representation

Example usage:
    from cleanup_cycle.clean_agent import clean_main, CleanMode
    
    results = clean_main(
        simulation_paths=["//server/sim1", "//server/sim2"],
        clean_mode=CleanMode.ANALYSE,
        num_sim_workers=32,
        output_path="./logs"
    )
"""

from cleanup_cycle.clean_agent.clean_main import clean_main
from cleanup_cycle.clean_agent.clean_parameters import CleanMode, CleanParameters
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressReporter, CleanProgressWriter
from cleanup_cycle.clean_agent.simulation_stubs import BaseSimulation, Simulation
from cleanup_cycle.clean_agent.thread_safe_counters import ThreadSafeCounter, ThreadSafeDeletionCounter, DeletionCounts

__all__ = [
    'clean_main',
    'CleanMode',
    'CleanParameters',
    'CleanProgressReporter',
    'CleanProgressWriter',
    'BaseSimulation',
    'Simulation',
    'ThreadSafeCounter',
    'ThreadSafeDeletionCounter',
    'DeletionCounts',
]
