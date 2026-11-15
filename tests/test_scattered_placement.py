"""Tests for JobPlacement._scattered_placement in placement/placement.py."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from placement.placement import JobPlacement  # noqa: E402


def test_scattered_placement_takes_only_the_gpus_still_needed():
    # Node 0 has one free GPU and node 1 has four. A 4-GPU job fits.
    free = {0: [3], 1: [4, 5, 6, 7]}
    gpus, found = JobPlacement(None)._scattered_placement({"num_GPUs": 4}, free)
    assert found
    assert gpus == [3, 4, 5, 6]


def test_scattered_placement_fails_only_when_too_few_gpus_are_free():
    free = {0: [3], 1: [4, 5]}
    gpus, found = JobPlacement(None)._scattered_placement({"num_GPUs": 4}, free)
    assert not found and gpus == []
