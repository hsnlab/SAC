# Copyright 2022 Janos Czentye
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import math

import networkx as nx

from alg.tree_rec_BTP import btp_tree_partitioning
from misc.generator import get_random_tree
from misc.util import evaluate_tree_partitioning
from service.common import NAME


def run_test(tree: nx.DiGraph, M: int, N: int, L: int, root: int = 1, cp_end: int = None, delay: int = 10,
             unit: int = 100):
    partition, opt_cost, opt_cut = btp_tree_partitioning(tree, root, M, N, L, cp_end, delay, unit)
    evaluate_tree_partitioning(tree, partition, opt_cost, root, cp_end, M, N, L, delay, unit)
    return partition, opt_cost, opt_cut


def test_tree_partitioning():
    tree = nx.read_gml("graph_test_tree.gml", destringizer=int)
    tree.graph[NAME] += "-seq_partition"
    M = 15
    N = 2
    L = math.inf
    root = 1
    cp_end = 10
    delay = 10
    run_test(**locals())


def test_random_tree_partitioning():
    tree = get_random_tree(10)
    tree.graph[NAME] += "-seq_partition"
    M = 6
    N = 2
    L = math.inf
    # L = 200
    root = 1
    cp_end = 10
    delay = 10
    run_test(**locals())


if __name__ == '__main__':
    test_tree_partitioning()
    test_random_tree_partitioning()
