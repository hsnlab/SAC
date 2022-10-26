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
import random

import networkx as nx

from alg.tree_meta_MTP import recreate_barr_blocks
from alg.util import recreate_subtrees, recreate_chain_blocks
from misc.algs import min_edge_weight_tree_partitioning, min_split_tree_clustering
from misc.generator import get_random_chain, get_random_tree
from misc.plot import draw_tree
from misc.util import print_tree_summary
from service.common import *


def test_chain_plotter():
    chain = get_random_chain()
    print_tree_summary(chain)
    barr = [1] + sorted(random.sample(range(2, len(chain)), len(chain) // 2))
    partition = recreate_chain_blocks(barr, len(chain))
    print("Partition", partition)
    draw_tree(chain, partition, draw_blocks=True, draw_weights=True)


def test_chain_tree_plotter():
    tree = nx.read_gml("tree/graph_test_tree.gml", destringizer=int)
    print_tree_summary(tree)
    barr = [1, 2, 6, 7, 9]
    partition = recreate_barr_blocks(tree, barr)
    print("Partition", partition)
    draw_tree(tree, partition, draw_blocks=True, draw_weights=True)


def test_random_tree_plotter():
    tree = get_random_tree()
    print_tree_summary(tree)
    barr = random.sample(list(n for n in tree.nodes if n != PLATFORM), len(tree) // 3)
    if 1 not in barr:
        barr.append(1)
    partition = recreate_subtrees(tree, barr)
    print("Partition", partition)
    draw_tree(tree, partition, draw_blocks=True, draw_weights=True)


def test_min_edge_partitioning():
    tree = nx.read_gml("tree/graph_test_tree.gml", destringizer=int)
    print_tree_summary(tree)
    barr = min_edge_weight_tree_partitioning(tree)
    partition = recreate_barr_blocks(tree, barr)
    print("Partition", partition)
    draw_tree(tree, partition, draw_blocks=True, draw_weights=True)


def test_min_split_tree_partitioning():
    tree = nx.read_gml("tree/graph_test_tree.gml", destringizer=int)
    # tree = get_random_tree()
    print_tree_summary(tree)
    partition = min_split_tree_clustering(tree, 4)
    print("Partition", partition)
    draw_tree(tree, partition, draw_blocks=True, draw_weights=True)


if __name__ == '__main__':
    # test_chain_plotter()
    test_chain_tree_plotter()
    test_random_tree_plotter()
    test_min_edge_partitioning()
    test_min_split_tree_partitioning()
