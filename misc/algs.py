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
import collections
import itertools
import operator

import networkx as nx

from alg.util import recreate_subtrees
from service.common import *


def min_edge_weight_tree_partitioning(sg: nx.DiGraph) -> list[int]:
    """
    Maximal edge-weight chain-based tree partitioning without memory constraint. The partitioning chooses the edge
    with the smallest weight at branching nodes.

    :param sg:  service graph annotated with node runtime(ms) and edge rate
    :return:    list of barrier nodes
    """
    return {1}.union(*(set(sg.succ[n]) - {max(sg.edges.data(RATE, 0, n), key=operator.itemgetter(2))[1]}
                       for n in sg.nodes if sg.degree[n] > 2))


def min_split_tree_clustering(sg: nx.DiGraph, k: int, full: bool = True) -> list[int]:
    """
    Minimal data-transfer tree clustering into *k* clusters without memory constraint. The clustering algorithm is based
    on the max split clustering algorithm(O(n^3)) which ranks the edges (paths) based on the amount of transferred data.

    :param sg:      service graph annotated with node runtime(ms), edge rate and edge data unit size
    :param k:       number of clusters
    :param full:    recreate ful blocks or return only the barrier nodes
    :return:        partitioning of the given service graph
    """
    D = {}
    dist_sg = nx.to_undirected(sg)
    # Define distance of two nodes as the reciprocal of the sum transferred data between the nodes
    for u, v in itertools.combinations((n for n in sg.nodes if n != PLATFORM), 2):
        D[u, v] = sum(1 / (d.get(RATE, 1) * D.get(DATA, 1))
                      for i, j, d in sg.edges(next(nx.all_simple_paths(dist_sg, u, v)), data=True))
    edges, rank = set(sg.edges((n for n in sg.nodes if n != PLATFORM))), 1
    labeled = collections.deque(maxlen=k)
    # Iterate paths from the min distant element
    for (i, j), _ in sorted(list(D.items()), key=operator.itemgetter(1)):
        path_edges = set(e for e in sg.edges(next(nx.all_simple_paths(dist_sg, i, j))))
        # If there is unlabelled edge on the given path
        if unlabeled := path_edges & edges:
            labeled.extend((rank, b) for _, b in unlabeled)
            edges -= unlabeled
            # If all edges are labeled -> stop
            if not edges:
                break
            rank += 1
    barr = {1}.union(b for e, b in labeled)
    return recreate_subtrees(sg, barr) if full else barr
