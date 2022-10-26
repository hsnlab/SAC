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
import math
import random
import time

import networkx as nx
import pandas as pd
import tabulate

from alg.tree_greedy import greedy_tree_partitioning
from alg.tree_meta_MTP import mtp_tree_partitioning
from alg.tree_rec_BTP import btp_tree_partitioning
from alg.util import ibacktrack_chain
from misc.generator import get_random_tree
from service.common import *


def run_all_tests(params: dict):
    tree_algs = dict(
        GREEDY=greedy_tree_partitioning,
        MTP=mtp_tree_partitioning,
        BTP=btp_tree_partitioning
    )
    ##########################################################
    stats = []
    for name, tree_alg in tree_algs.items():
        t_start = time.process_time()
        result = tree_alg(**params)
        alg_time = time.process_time() - t_start
        if name == 'GREEDY':
            stats.extend([[name + f'_{i}', *res, alg_time] for i, res in enumerate(result)])
        else:
            stats.append([name, *result, alg_time])
    return stats


def compare_results(tree_path: str = None):
    tree = nx.read_gml(tree_path if tree_path is not None else "tree/graph_test_tree.gml", destringizer=int)
    params = dict(sg=tree, M=15, N=2, L=math.inf, root=1, cp_end=10, delay=10)
    ##########################################################
    stats = run_all_tests(params)
    print("Summary:")
    print(tabulate.tabulate(stats, ['Alg.', 'Partition', 'Cost', 'Lat/Cut', 'Time'],
                            numalign='center', stralign='left', tablefmt='pretty'))


def test_latencies():
    sg = nx.read_gml("tree/graph_test_tree_latency.gml", destringizer=int)
    M = 15
    N = 3
    L = math.inf
    root = 1
    cp_end = 10
    delay = 10
    params = locals()
    lats = [math.inf,
            # Optimal solution
            sum(sg.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 3,
            # Forces to reduce blocks
            sum(sg.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 2,
            # Stricter latency
            sum(sg.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 1,
            # Strictest latency
            sum(sg.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) + delay * 0,
            # Infeasible latency
            sum(sg.nodes[v][RUNTIME] for v in (1, 3, 8, 10)) - 1]
    print(sg.graph.get(NAME, "tree").center(80, '#'))
    print("Runtime:", [sg.nodes[v][RUNTIME] for v in sg.nodes if v != PLATFORM])
    print("Memory:", [sg.nodes[v][MEMORY] for v in sg.nodes if v != PLATFORM])
    print("Rate:", [sg[next(sg.predecessors(v))][v][RATE] for v in sg.nodes if v != PLATFORM])
    for lat in lats:
        params['L'] = lat
        stat = run_all_tests(params)
        print("Params:", repr(params))
        print(tabulate.tabulate(stat, ['Alg.', 'Partition', 'Cost', 'Lat/Cut', 'Time'],
                                numalign='center', stralign='left', tablefmt='pretty'))
        print('#' * 80)


def test_random_validation(n: int = 10, cache_failed: bool = False, stop_failed=False):
    sg = get_random_tree(n)
    cp_end = n
    cpath = list(reversed(list(ibacktrack_chain(sg, 1, cp_end))))
    l_min = sum(sg.nodes[v][RUNTIME] for v in cpath)
    rand_cut = random.randint(2, len(cpath) - 1)
    params = dict(sg=sg, M=6, N=2, root=1, cp_end=cp_end, delay=10, L=l_min + 10 * rand_cut)
    print(sg.graph.get(NAME, "tree").center(80, '#'))
    print("Runtime:", [sg.nodes[v][RUNTIME] for v in sg.nodes if v != PLATFORM])
    print("Memory:", [sg.nodes[v][MEMORY] for v in sg.nodes if v != PLATFORM])
    print("Rate:", [sg[next(sg.predecessors(v))][v][RATE] for v in sg.nodes if v != PLATFORM])
    print(f"Tree partitioning [M={params['M']}, L={params['L']}:{(1, cp_end)}] -> cpath:{cpath}, min_lat:{l_min}")
    stat = run_all_tests(params)
    print("Params:", repr(params))
    print(tabulate.tabulate(stat, ['Alg.', 'Partition', 'Cost', 'Lat/Cut', 'Time'],
                            numalign='center', stralign='right', tablefmt='pretty'))
    print('#' * 80)
    greedy_parts, meta_part, seq_part = [p[1] for p in stat[:-2]], stat[-2][1], stat[-1][1]
    greedy_cost, meta_cost, seq_cost = stat[0][2], stat[-2][2], stat[-1][2]
    validated = (meta_part in greedy_parts and seq_part in greedy_parts) and (
        meta_cost == seq_cost == greedy_cost if meta_cost and seq_cost and greedy_cost else True)
    if not validated and cache_failed:
        sg.graph[NAME] = f"failed_tree_{sg.graph[NAME]}.gml"
        nx.write_gml(sg, sg.graph[NAME])
    result = 'SUCCESS' if validated else 'FAILED'
    print(f"Validation: {result}")
    if stop_failed:
        assert result == 'SUCCESS'
    print('#' * 80)
    return result, stat


def stress_test(n: int = 10, iteration: int = 100):
    results = [test_random_validation(n, cache_failed=True, stop_failed=False) for _ in range(iteration)]
    valid, stats = zip(*results)
    print("Validation statistics:", collections.Counter(valid))
    df = pd.DataFrame(itertools.chain(*stats))
    pd.set_option('display.expand_frame_repr', False)
    print(df[(df[2] < math.inf) & (df[0].isin(('GREEDY_0', 'MTP', 'BTP')))][[0, 4]].groupby(0).describe())


if __name__ == '__main__':
    compare_results()
    # test_latencies()
    # test_random_validation()
    # compare_results("failed_tree_1658172734.3944385.gml")
    stress_test(n=10, iteration=100)
