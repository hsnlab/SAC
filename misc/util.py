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
import tabulate

from alg.util import block_memory, block_latency, path_blocks, block_cost, block_cpu, label_nodes, ichain
from misc.plot import draw_tree
from service.common import *


def print_tree_summary(sg: nx.DiGraph):
    """Print summary of service graphs"""
    print(sg)
    for n, nd in sg.nodes(data=True):
        print(f"\t{n}: {nd}")
        for i, j, ed in sg.out_edges(n, data=True):
            print(f"\t\t{i} -> {j}: {ed}")


def get_chain_k_min(memory: list[int], M: int, rate: list[int], N: int, start: int = 0, end: int = None) -> int:
    """Return minimal number of blocks due to constraint M and N"""
    end = end if end is not None else len(memory) - 1
    return max(math.ceil(block_memory(memory, start, end) / M),
               sum(1 for i, j in itertools.pairwise(rate[start: end + 1]) if math.ceil(j / i) > N))


def get_chain_c_max(runtime: list[int], L: int, b: int, w: int, delay: int, start: int = 0, end: int = None) -> int:
    """Return maximal number of blocks due to constraint L"""
    end = end if end is not None else len(runtime) - 1
    return math.floor(min((L - block_latency(runtime, b, w, delay, start, end)) / delay, len(runtime) - 1))


def get_chain_k_max(runtime: list[int], L: int, b: int, w: int, delay: int, start: int = 0, end: int = None) -> int:
    """Return maximal number of blocks due to constraint L"""
    return get_chain_c_max(runtime, L, b, w, delay, start, end) + 1


def print_block_stat(partition: list[list[int]], runtime: list[int], memory: list[int], rate: list[int], delay: float,
                     start: int = 0, end: int = None, unit: int = 100):
    end = end if end is not None else len(runtime) - 1
    stat = [[str([blk[0], blk[-1]]),
             block_cost(runtime, rate, blk[0], blk[-1], unit),
             block_memory(memory, blk[0], blk[-1]),
             block_cpu(rate, blk[0], blk[-1]),
             block_latency(runtime, blk[0], blk[-1], delay, start, end)] for blk in partition]
    print(tabulate.tabulate(stat, ['Block', 'Cost', 'Memory', 'CPU', 'Latency'],
                            numalign='decimal', stralign='center', tablefmt='pretty'))


def evaluate_chain_partitioning(partition: list, opt_cost: int, opt_lat: int, runtime: list, memory: list, rate: list,
                                M: int = math.inf, N: int = math.inf, L: int = math.inf, start: int = 0,
                                end: int = None, delay: int = 1, unit: int = 100):
    print('#' * 80)
    print(f"Chain partitioning [M={M}, N={N}, L={L}:{(start, end)}] => "
          f"{partition} - opt_cost: {opt_cost}, opt_lat: {opt_lat}")
    print(f"k_min={get_chain_k_min(memory, M, rate, N, start, end)}, "
          f"k_opt[L]={len(path_blocks(partition, range(start, end + 1)))}, "
          f"k_max={get_chain_k_max(runtime, L, 0, len(runtime), delay, start, end)}")
    print_block_stat(partition, runtime, memory, rate, delay, start, end, unit)
    print('#' * 80)


def print_chain_summary(runtime: list[int], memory: list[int], rate: list[int]):
    print("Chain:", "[", *(f"-{r}-> F({t}|M{m})" for t, m, r in zip(runtime, memory, rate)), "]")


def print_cpath_stat(sg: nx.DiGraph, partition: list[list[int]], cpath: list[int] = None, delay: int = 10):
    """Print the related block of the critical path and """
    if len(partition) > 0:
        c_blocks = path_blocks(partition, cpath)
        opt_cut = len(c_blocks) - 1
        sum_lat = sum(block_latency([sg.nodes[v][RUNTIME] for v in blk], 0, len(blk) - 1, delay, 0, len(blk) - 1)
                      for blk in c_blocks) + opt_cut * delay
        print("Critical blocks of cpath", [cpath[0], cpath[-1]], "=>", c_blocks, "-", "opt_cut:", opt_cut, "-",
              "opt_lat:", sum_lat)


def print_tree_block_stat(sg: nx.DiGraph, partition: list[list[int]], unit: int = 100):
    """Print cost memory and latency values of partition blocks in tabulated format"""
    stat = []
    for blk in partition:
        pred = next(sg.predecessors(blk[0]))
        runtime, memory, rate = zip(*[(sg.nodes[v][RUNTIME], sg.nodes[v][MEMORY], sg[u][v][RATE])
                                      for u, v in itertools.pairwise([pred] + blk)])
        b, w = 0, len(blk) - 1
        stat.append([str([blk[b], blk[w]]),
                     block_cost(runtime, rate, b, w, unit),
                     block_memory(memory, b, w),
                     block_cpu(rate, b, w),
                     block_latency(runtime, b, w, 0, b, w)])
    print(tabulate.tabulate(stat, ['Block', 'Cost', 'Memory', 'CPU', 'Latency'], numalign='decimal', stralign='center'))


def evaluate_tree_partitioning(tree: nx.DiGraph, partition: list[list[int]], opt_cost: int, root: int, cp_end: int,
                               M: int, N: int, L: int, delay: int, unit: int):
    tree = label_nodes(tree)
    print(tree.graph.get(NAME, "tree").center(80, '#'))
    print("Runtime:", [tree.nodes[v][RUNTIME] for v in tree.nodes if v != PLATFORM])
    print("Memory:", [tree.nodes[v][MEMORY] for v in tree.nodes if v != PLATFORM])
    print("Rate:", [tree[next(tree.predecessors(v))][v][RATE] for v in tree.nodes if v != PLATFORM])
    print(f"Tree partitioning [M={M}, N={N}, L={L}:{(root, cp_end)}] => {partition} - opt_cost: {opt_cost}")
    if partition:
        print_cpath_stat(tree, partition, list(ichain(tree, root, cp_end)), delay)
        print_tree_block_stat(tree, partition, unit)
        draw_tree(tree, partition, draw_blocks=True, draw_weights=False)
    print('#' * 80)
