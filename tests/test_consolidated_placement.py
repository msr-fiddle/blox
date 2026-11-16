"""Tests for JobPlacement._consolidated_placement in placement/placement.py."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from placement.placement import JobPlacement  # noqa: E402


def test_uses_a_node_with_spare_gpus_when_none_fits_exactly():
    # No node has exactly two free GPUs, but node 1 has four. A 2-GPU job fits there.
    free = {0: [3], 1: [4, 5, 6, 7]}
    gpus, found = JobPlacement(None)._consolidated_placement({"num_GPUs": 2}, free)
    assert found
    assert gpus == [4, 5]


def test_prefers_the_node_with_the_fewest_spare_gpus():
    free = {0: [0, 1, 2, 3], 1: [4, 5, 6]}
    gpus, found = JobPlacement(None)._consolidated_placement({"num_GPUs": 2}, free)
    assert found
    assert gpus == [4, 5]


def test_fails_only_when_no_node_is_big_enough():
    free = {0: [0], 1: [4]}
    gpus, found = JobPlacement(None)._consolidated_placement({"num_GPUs": 2}, free)
    assert not found and gpus == []
