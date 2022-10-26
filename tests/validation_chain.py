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
import json
import math
import random
import time

import pandas as pd
import tabulate

from misc.util import print_chain_summary
from chain.test_chain_dp_scp import run_test as dp_chain_test
from chain.test_chain_greedy import run_test as greedy_chain_test
from chain.test_chain_vec_scp import run_test as vec_chain_test


def run_all_tests(params: dict):
    chain_algs = dict(
        GREEDY=greedy_chain_test,
        SCP=dp_chain_test,
        VEC_SCP=vec_chain_test
    )
    ##########################################################
    stats = []
    for name, chain_alg in chain_algs.items():
        t_start = time.process_time()
        result = chain_alg(**params)
        alg_time = time.process_time() - t_start
        if name == 'GREEDY':
            stats.extend([[name + f'_{i}', *res, alg_time] for i, res in enumerate(result)])
        else:
            stats.append([name, *result, alg_time])
    return stats


def compare_results(chain_path: str = None):
    if chain_path:
        with open(chain_path) as f:
            params = json.load(f)
    else:
        runtime = [20, 40, 50, 20, 70, 40, 50, 60, 40, 10]
        params = dict(runtime=runtime,
                      memory=[3, 3, 2, 1, 2, 1, 2, 1, 2, 3],
                      rate=[1, 1, 2, 2, 1, 3, 1, 2, 1, 3],
                      delay=10,
                      M=6,
                      N=2,
                      L=390,
                      start=1,
                      end=8,
                      unit=100)
    # params['L'] = math.inf
    stats = run_all_tests(params)
    print("Summary:")
    print(tabulate.tabulate(stats, ['Alg.', 'Partition', 'Cost', 'Lat/Cut', 'Time'],
                            numalign='center', stralign='left', tablefmt='pretty'))


def test_latencies():
    runtime = [20, 40, 50, 20, 70, 40, 50, 60, 40, 10]
    params = dict(runtime=runtime,
                  memory=[3, 3, 2, 1, 2, 1, 2, 1, 2, 3],
                  rate=[1, 1, 2, 2, 1, 3, 1, 2, 1, 3],
                  delay=10,
                  M=6,
                  N=3,
                  start=1,
                  end=8,
                  unit=100)
    lats = [math.inf,
            # No restriction
            sum(runtime[params['start']:params['end'] + 1]) + params['delay'] * 4,
            # Optimal
            sum(runtime[params['start']:params['end'] + 1]) + params['delay'] * 3,
            # Forces to reduce blocks
            sum(runtime[params['start']:params['end'] + 1]) + params['delay'] * 2,
            # Infeasible due to M
            sum(runtime[params['start']:params['end'] + 1]) + params['delay'] * 1]
    print_chain_summary(params['runtime'], params['memory'], params['rate'])
    for lat in lats:
        params['L'] = lat
        print_chain_summary(params['runtime'], params['memory'], params['rate'])
        stat = run_all_tests(params)
        print("  Statistics  ".center(80, '#'))
        print("Params:", repr(params))
        print(tabulate.tabulate(stat, ['Alg.', 'Partition', 'Cost', 'Lat/Cut', 'Time'],
                                numalign='center', stralign='left', tablefmt='pretty'))
        greedy_parts, dp_part, vec_part = [p[1] for p in stat[:-2]], stat[-2][1], stat[-1][1]
        validated = bool(dp_part in greedy_parts and vec_part in greedy_parts)
        result = 'SUCCESS' if validated else 'FAILED'
        print(f"Validation: {result}")
        print('#' * 80)


def test_random_validation(n: int = 10, cache_failed: bool = True, stop_failed: bool = False):
    runtime = [random.randint(10, 100) for _ in range(n)]
    delay = 10
    params = dict(runtime=runtime,
                  memory=[random.randint(1, 3) for _ in range(n)],
                  rate=[1, *(random.randint(1, 3) for _ in range(n-1))],
                  delay=delay,
                  M=6,
                  N=2,
                  L=sum(runtime) + 1 + delay * random.randint(len(runtime) // 4, len(runtime) // 2),
                  start=0,
                  end=len(runtime) - 1,
                  unit=100)
    print_chain_summary(params['runtime'], params['memory'], params['rate'])
    stat = run_all_tests(params)
    print("  Statistics  ".center(80, '#'))
    print("Params:", repr(params))
    print(tabulate.tabulate(stat, ['Alg.', 'Partition', 'Cost', 'Lat/Cut', 'Time'],
                            numalign='center', stralign='left', tablefmt='pretty'))
    greedy_parts, dp_part, vec_part = [p[1] for p in stat[:-2]], stat[-2][1], stat[-1][1]
    greedy_cost, dp_cost, vec_cost = stat[0][2], stat[-2][2], stat[-1][2]
    validated = (dp_part in greedy_parts and vec_part in greedy_parts) and (
        greedy_cost == dp_cost == vec_cost if dp_cost and vec_cost and greedy_cost else True)
    if not validated and cache_failed:
        with open(f"failed_chain_{time.time()}.json", 'w') as f:
            json.dump(params, f, indent=4)
    result = 'SUCCESS' if validated else 'FAILED'
    print(f"Validation: {result}")
    if stop_failed:
        assert result == 'SUCCESS'
    print('#' * 80)
    return result, stat


def stress_test(n: int = 10, iteration: int = 100):
    results = [test_random_validation(n, cache_failed=False, stop_failed=False) for _ in range(iteration)]
    valid, stats = zip(*results)
    print("Validation statistics:", collections.Counter(valid))
    df = pd.DataFrame(itertools.chain(*stats))
    pd.set_option('display.expand_frame_repr', False)
    print("Runtime statistics:")
    print(df[(df[2] < math.inf) & (df[0].isin(('GREEDY_0', 'SCP', 'VEC_SCP')))][[0, 4]].groupby(0).describe())


if __name__ == '__main__':
    compare_results()
    # test_latencies()
    # test_random_validation(n=100)
    # compare_results("failed_chain_1659004289.5830414.json")
    stress_test(10, iteration=100)
