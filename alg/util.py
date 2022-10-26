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
import functools
import itertools
import math

import networkx as nx

from service.common import LABEL, PLATFORM

# Constants for attribute indices in DP matrix
BARR, COST, LAT = 0, 1, 2
# Constant for block cache index
MEM, CPU = 0, 3


def ipowerset(data: list[int], start: int = 0) -> list[int]:
    """Generate the powerset of the given *data* beginning to count the sets from size *start*"""
    return itertools.chain.from_iterable(itertools.combinations(data, i) for i in range(start, len(data) + 1))


def recreate_chain_blocks(barr: list[int], n: int, full: bool = True) -> list[list[int]]:
    """Recreate partition blocks from barrier nodes for an n-size chain := [0, n-1]"""
    return [list(range(b, w)) if full else [b, w - 1] if b < w - 1 else [b]
            for b, w in itertools.pairwise(barr + [n])]


def block_memory(memory: list[int], b: int, w: int) -> int:
    """Calculate memory of block [b,w]"""
    return sum(memory[b: w + 1])


def block_cost(runtime: list[int], rate: list[int], b: int, w: int, unit: int = 100) -> int:
    """Calculate running time of block [b,w]"""
    return rate[b] * (math.ceil(sum(runtime[b: w + 1]) / unit) * unit)


def block_latency(runtime: list[int], b: int, w: int, delay: int, start: int, end: int) -> int:
    """Calculate relevant latency for block [b,w]"""
    if end < b or w < start:
        # Do not consider latency if no intersection
        return 0
    blk_lat = sum(runtime[max(b, start): min(w, end) + 1])
    # Ignore delay if latency path starts within the subchain
    return delay + blk_lat if start < b else blk_lat


def block_cpu(rate: list[int], b: int, w: int) -> int:
    """Calculate CPU core need of block [b,w]"""
    r_max = itertools.chain((1,), enumerate(itertools.accumulate(reversed(rate[b: w + 1]), max)))
    return functools.reduce(lambda pre, max_i: max(pre, math.ceil(max_i[1] / rate[w - max_i[0]])), r_max)


def isubtrees(tree: nx.DiGraph, barr: set[int]) -> tuple[int, nx.DiGraph]:
    """Return the barrier nodes and subtrees of the given *tree* marked by the *barr* nodes"""
    for b in barr:
        nodes = [b]
        children = collections.deque(nodes)
        while children:
            u = children.popleft()
            for v in tree.succ[u]:
                if v not in barr:
                    nodes.append(v)
                    children.append(v)
        yield b, tree.subgraph(sorted(nodes))


def ibacktrack_chain(tree: nx.DiGraph, start: int, leaf: int) -> list[int]:
    """Return the node of a chain in the *tree* in backward order from *leaf* to *start* node"""
    last = leaf
    while last != start:
        yield last
        try:
            last = next(tree.predecessors(last))
        except StopIteration:
            break
    yield last


def path_blocks(partition: list[list[int]], path: list[int]) -> list[list[int]]:
    """Calculate the blocks of separated critical path based on the original partitioning"""
    parts = []
    current_blk = None
    for v in path:
        for blk in partition:
            if v in blk:
                if blk == current_blk:
                    parts[-1].append(v)
                else:
                    parts.append([v])
                    current_blk = blk
    return parts


def ipostorder_dfs(tree: nx.DiGraph, source: int) -> tuple[int, int]:
    """Return nodes and its existing predecessor in DFS traversal of the given *tree* in a post/reversed order"""
    stack = collections.deque([(source, iter(tree[source]))])
    while stack:
        v, ichildren = stack[-1]
        try:
            c = next(ichildren)
            stack.append((c, iter(tree[c])))
        except StopIteration:
            stack.pop()
            if stack:
                yield stack[-1][0], v


def isubchains(tree: nx.DiGraph, start: int, leaf: int = None) -> tuple[(list[int], list[int]), set[int]]:
    """Generator over the subchains and its branches from *start* to all reachable leaf where the subchain is bisected
        at the last node from which the specific *leaf* is still reachable"""
    chain = [start]
    while (deg := len(tree.succ[chain[-1]])) == 1:
        chain.append(next(tree.successors(chain[-1])))
    if deg == 0:
        yield (chain, []), set()
    else:
        for c in (children := set(tree.successors(chain[-1]))):
            nbr = children - {c}
            for (part1, part2), branches in isubchains(tree, c, leaf):
                if leaf in tree.nodes[part1[0]][LABEL]:
                    yield (chain + part1, part2), nbr | branches
                elif leaf is not None:
                    yield (chain, part1 + part2), nbr | branches
                else:
                    yield (chain + part1, []), nbr | branches


def extract_barriers(DP: list[list[tuple[int, int, int]]], k: int) -> list[int]:
    """General function for extracting barrier nodes form *DP* matrix by iteratively backtracking subcases from *k*"""
    barr = []
    w = len(DP) - 1
    for k in reversed(range(0, k + 1)):
        # The cached b value marks the barrier node of the k. block and refers the subcase => C[b-1,k-1] + c[b,w]
        b = DP[w][k][BARR]
        w = b - 1
        barr.append(b)
    barr.reverse()
    return barr


def label_nodes(tree: nx.DiGraph) -> nx.DiGraph:
    """Label each node *n* with the set of leafs that can be reached from *n*"""
    for _, n in ipostorder_dfs(tree, PLATFORM):
        tree.nodes[n][LABEL] = set().union(*(tree.nodes[m][LABEL] for m in tree.succ[n])) if len(tree.succ[n]) else {n}
    return tree


def recreate_subtrees(tree: nx.DiGraph, barr: set[int]) -> list[list[int]]:
    """Return the nodes of the subtrees defined by the *barr* nodes as the root of the subtree"""
    return sorted(sorted(list(st.nodes)) for _, st in isubtrees(tree, barr))


def ichain(tree: nx.DiGraph, start: int, leaf: int) -> list[int]:
    """Generator over the nodes of the chain from *start* node to *leaf* node"""
    n = start
    while n != leaf:
        yield n
        for c in tree.successors(n):
            if leaf in tree.nodes[c][LABEL]:
                n = c
                break
    yield leaf
