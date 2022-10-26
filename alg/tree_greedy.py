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
import itertools
import math

import networkx as nx

from alg.util import (isubtrees, ibacktrack_chain, ipowerset, path_blocks,
                      block_cost, block_latency, block_memory, block_cpu)
from service.common import MEMORY, RUNTIME, RATE, PLATFORM


def ichains_exhaustive(sg: nx.DiGraph, root: int, M: int, N: int) -> list[int]:
    """Calculate all combination of edge cuts and returns only if it is feasible wrt. the chain connectivity, M, and N.
    The calculation is improved compared to brute force to only start calculating cuts from c_min.

    :param sg:      service graph annotated with node runtime(ms), memory(MB) and edge rate
    :param root:    root node of the graph
    :param M:       upper memory bound in MB
    :param N:       upper CPU core bound
    :return:        generator of chain partitions
    """
    c_min = math.ceil(sum(nx.get_node_attributes(sg, MEMORY).values()) / M) - 1
    for cuts in ipowerset(sg.edges(range(1, len(sg))), start=c_min):
        barr = {root}.union(v for u, v in cuts)
        # Check whether the subtrees are chains and meet the memory requirement M and N
        for b, subtree in isubtrees(sg, barr):
            if max(d for _, d in subtree.out_degree) > 1:
                break
            memory, rate = zip(*[(sg.nodes[v][MEMORY], sg[u][v][RATE]) for u, v in
                                 itertools.pairwise([next(sg.predecessors(b)), *sorted(list(subtree.nodes))])])
            if block_memory(memory, 0, len(subtree) - 1) > M or block_cpu(rate, 0, len(subtree) - 1) > N:
                break
        else:
            yield barr


def ifeasible_chains(sg: nx.DiGraph, root: int, M: int, N: int) -> list[int]:
    """Calculate only feasible chain partitions and returns the one which meets the limits M and N.
    The calculation is improved compared to brute force to only calculate chain partitions based on the branching nodes.

    :param sg:      service graph annotated with node runtime(ms), memory(MB) and edge rate
    :param root:    root node of the graph
    :param M:       upper memory bound in MB
    :param N:       upper CPU core bound
    :return:        generator of chain partitions
    """
    branch_edges = [itertools.chain(itertools.combinations(sg.succ[b], len(sg.succ[b]) - 1), [tuple(sg.successors(b))])
                    for b in (v for v, d in sg.out_degree if d > 1)]
    single_edges = ipowerset([v for v in sg.nodes if v != PLATFORM and sg.degree(next(sg.predecessors(v))) == 2])
    for chain_cuts in itertools.product(*branch_edges, single_edges):
        barr = {root}.union(itertools.chain.from_iterable(chain_cuts))
        # Check whether the subtrees are chains and meet the memory requirement M and N
        for b, subtree in isubtrees(sg, barr):
            memory, rate = zip(*[(sg.nodes[v][MEMORY], sg[u][v][RATE]) for u, v in
                                 itertools.pairwise([next(sg.predecessors(b)), *sorted(list(subtree.nodes))])])
            if block_memory(memory, 0, len(subtree) - 1) > M or block_cpu(rate, 0, len(subtree) - 1) > N:
                break
        else:
            yield barr


def greedy_tree_partitioning(sg: nx.DiGraph, root: int = 1, M: int = math.inf, N: int = math.inf,
                             L: int = math.inf, cp_end: int = None, delay: int = 1, unit: int = 100,
                             ichains=ifeasible_chains) -> list[tuple[list, int, int]]:
    """
    Calculates minimal-cost partitioning of a service graph(tree) by iterating over all possible cuttings.

    :param sg:      service graph annotated with node runtime(ms), memory(MB) and edge rate
    :param root:    root node of the graph
    :param M:       upper memory bound of the partition blocks (in MB)
    :param N:       upper CPU core bound of the partition blocks
    :param L:       latency limit defined on the critical path (in ms)
    :param cp_end:  tail node of the critical path in the form of subchain[root -> cp_end]
    :param delay:   invocation delay between blocks
    :param unit:    rounding unit for the cost calculation (default: 100 ms)
    :param ichains: generator of chain partitions
    :return:        tuple of list of best partitions, sum cost of the partitioning, and resulted latency
    """
    best_res, best_cost = [([], math.inf, None)], math.inf
    # Iterates over all possible cuttings
    for barr in ichains(sg, root, M, N):
        partition = []
        sum_cost = 0
        for b, st in isubtrees(sg, barr):
            partition.append(sorted(list(st.nodes)))
            runtime, rate = zip(*[(sg.nodes[v][RUNTIME], sg[u][v][RATE])
                                  for u, v in itertools.pairwise([next(sg.predecessors(b)), *partition[-1]])])
            sum_cost += block_cost(runtime, rate, 0, len(partition[-1]) - 1, unit)
        # Calculate blocks of critical path based on the partitioning
        cp_block = path_blocks(partition, reversed(list(ibacktrack_chain(sg, root, cp_end))))
        sum_lat = sum(block_latency([sg.nodes[v][RUNTIME] for v in blk], 0, len(blk) - 1, delay, 0, len(blk) - 1)
                      for blk in cp_block) + (len(cp_block) - 1) * delay
        partition.sort()
        if sum_lat <= L:
            # Store partitioning with the same best cost for comparison
            if sum_cost == best_cost:
                best_res.append((partition, sum_cost, sum_lat))
            # Initialize new best cost partitioning
            elif sum_cost < best_cost:
                best_res, best_cost = [(partition, sum_cost, sum_lat)], sum_cost
    return best_res
