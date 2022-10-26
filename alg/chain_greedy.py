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

from alg.util import ipowerset, recreate_chain_blocks, block_memory, block_cost, block_latency, block_cpu


def ichain_blocks(memory: list[int], M: int, rate: list[int], N: int) -> list[list[list[int]]]:
    """
    Calculates all combination of chain cuts with respect to the *memory* values and constraint *M*.
    The calculation is improved compared to brute force to only start calculating cuts from c_min.
    """
    n = len(memory)
    for cut in ipowerset(range(1, n), start=math.ceil(sum(memory) / M) - 1):
        barr = sorted({0}.union(cut))
        # Consider only block with appropriate size
        valid = [blk for blk in recreate_chain_blocks(barr, n)
                 if block_memory(memory, blk[0], blk[-1]) <= M and block_cpu(rate, blk[0], blk[-1]) <= N]
        if len(valid) == len(barr):
            yield valid


def greedy_chain_partitioning(runtime: list, memory: list, rate: list, M: int = math.inf, N: int = math.inf,
                              L: int = math.inf, start: int = 0, end: int = None, delay: int = 1,
                              unit: int = 100) -> list[tuple[list[int], int, int]]:
    """
    Calculates the minimal-cost partitioning of a given chain by exhaustive search

    :param runtime: running times in ms
    :param memory:  memory requirements in MB
    :param rate:    avg. rate of function invocations
    :param M:       upper memory bound of the partition blocks (in MB)
    :param N:       upper CPU core bound of the partition blocks
    :param L:       latency limit defined on the critical path in the form of subchain[start -> end] (in ms)
    :param delay:   invocation delay between blocks
    :param start:   head node of the latency-limited subchain
    :param end:     tail node of the latency-limited subchain
    :param unit:    rounding unit for the cost calculation (default: 100 ms)
    :return:        list if min-cost partitions, related optimal cost and latency
    """
    best_res, best_cost = [([], math.inf, None)], math.inf
    for partition in ichain_blocks(memory, M, rate, N):
        if (sum_lat := sum(block_latency(runtime, blk[0], blk[-1], delay, start, end) for blk in partition)) > L:
            continue
        elif (sum_cost := sum(block_cost(runtime, rate, blk[0], blk[-1], unit) for blk in partition)) == best_cost:
            best_res.append((partition, sum_cost, sum_lat))
        elif sum_cost < best_cost:
            best_res, best_cost = [(partition, sum_cost, sum_lat)], sum_cost
    return best_res
