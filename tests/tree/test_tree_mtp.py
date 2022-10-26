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

from alg.tree_meta_MTP import label_nodes, isubchains, mtp_tree_partitioning
from alg.util import ichain
from misc.generator import get_random_tree
from misc.plot import draw_tree
from misc.util import print_tree_summary, evaluate_tree_partitioning
from service.common import NAME, RUNTIME


def run_test(tree: nx.DiGraph, M: int, N: int, L: int, root: int = 1, cp_end: int = None, delay: int = 10,
             unit: int = 100):
    partition, opt_cost, opt_lat = mtp_tree_partitioning(tree, root, M, N, L, cp_end, delay, unit)
    # partition = recreate_barr_blocks(tree, barr) if barr else []
    evaluate_tree_partitioning(tree, partition, opt_cost, root, cp_end, M, N, L, delay, unit)
    return partition, opt_cost, opt_lat


def test_node_labeling():
    labeled_tree = label_nodes(get_random_tree(10))
    draw_tree(labeled_tree)
    print_tree_summary(labeled_tree)


def test_chain_pruning():
    labeled_tree = label_nodes(get_random_tree(10))
    draw_tree(labeled_tree)
    for chain, m in isubchains(labeled_tree, 1):
        print(f"Subchain: {chain}, branches: {m}")


def test_cp_chain():
    labeled_tree = label_nodes(get_random_tree(10))
    draw_tree(labeled_tree)
    print(list(ichain(labeled_tree, 1, 10)))


def test_tree_partitioning():
    tree = nx.read_gml("graph_test_tree.gml", destringizer=int)
    tree.graph[NAME] += "-meta_partition"
    M = 15
    N = 2
    L = math.inf
    root = 1
    cp_end = 10
    delay = 10
    run_test(**locals())


def test_random_tree_partitioning():
    tree = get_random_tree(10)
    tree.graph[NAME] += "-meta_partition"
    M = 6
    N = 2
    L = math.inf
    root = 1
    cp_end = 10
    delay = 10
    run_test(**locals())


def test_tree_partitioning_latency():
    tree = nx.read_gml("graph_test_tree_latency.gml", destringizer=int)
    tree.graph[NAME] += "-seq_partition"
    M = 6
    N = 3
    root = 1
    cp_end = 10
    delay = 10
    # No restriction
    L = math.inf
    run_test(**locals())
    # Optimal solution
    L = sum(tree.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 3
    run_test(**locals())
    # Forces to reduce blocks
    L = sum(tree.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 2
    run_test(**locals())
    # Stricter latency
    L = sum(tree.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 1
    run_test(**locals())
    # Infeasible due to M
    L = sum(tree.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 0
    run_test(**locals())


if __name__ == '__main__':
    # test_node_labeling()
    # test_chain_pruning()
    # test_cp_chain()
    test_tree_partitioning()
    test_random_tree_partitioning()
    test_tree_partitioning_latency()
