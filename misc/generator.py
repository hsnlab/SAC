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
import time

import networkx as nx

from service.common import *


def get_random_chain(nodes: int = 10, runtime: tuple[int, int] = (1, 100), memory: tuple[int, int] = (1, 3),
                     rate: tuple[int, int] = (1, 3), data: tuple[int, int] = (1, 20)) -> nx.DiGraph:
    """Generate random chain(path graph) with properties from given intervals"""
    chain = nx.path_graph(range(0, nodes + 1), nx.DiGraph)
    nx.set_node_attributes(chain, {i: random.randint(*runtime) for i in range(1, nodes + 1)}, RUNTIME)
    nx.set_node_attributes(chain, {i: random.randint(*memory) for i in range(1, nodes + 1)}, MEMORY)
    for _, _, d in chain.edges(data=True):
        d[RATE] = random.randint(*rate)
        d[DATA] = random.randint(*data)
    chain = nx.relabel_nodes(chain, {0: PLATFORM})
    chain.graph[NAME] = "random_chain"
    return chain


def get_random_tree(nodes: int = 20, runtime: tuple[int, int] = (1, 100), memory: tuple[int, int] = (1, 3),
                    rate: tuple[int, int] = (1, 3), data: tuple[int, int] = (1, 20)) -> nx.DiGraph:
    """Generate random tree(from Prüfer sequence) with properties from given intervals"""
    raw_tree = nx.bfs_tree(nx.random_tree(nodes + 1), 0)
    while raw_tree.out_degree[0] > 1:
        raw_tree = nx.bfs_tree(nx.random_tree(nodes + 1), 0)
    tree = nx.convert_node_labels_to_integers(raw_tree, first_label=0)
    nx.set_node_attributes(tree, {i: random.randint(*runtime) for i in range(1, nodes + 1)}, RUNTIME)
    nx.set_node_attributes(tree, {i: random.randint(*memory) for i in range(1, nodes + 1)}, MEMORY)
    for _, _, d in tree.edges(data=True):
        d[RATE] = random.randint(*rate)
        d[DATA] = random.randint(*data)
    tree = nx.relabel_nodes(tree, {0: PLATFORM})
    tree.graph[NAME] = f"random_tree_{time.time()}"
    return tree
