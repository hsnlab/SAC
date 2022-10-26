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

import numpy as np

from alg.util import MEM, CPU, COST, LAT, BARR


def vec_chain_partitioning(runtime: list, memory: list, rate: list, M: int = np.inf, N: int = np.inf, L: int = np.inf,
                           start: int = 0, end: int = None, delay: int = 1, unit: int = 100,
                           ret_dp: bool = False) -> tuple[list, int, int]:
    """
    Calculates minimal-cost partitioning of a chain based on the node properties of *runtime*, *memory* and *rate* with
    respect to an upper bound **M** on the total memory of blocks and a latency constraint **L** defined on the subchain
    between *start* and *end* nodes leveraging vectorized operations.

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
    :param ret_dp:  return the calculated DP matrix instead of the barrier nodes
    :return:        tuple of barrier nodes, sum cost of the partitioning, and the calculated latency on the subchain
    """
    n = len(runtime)
    end = end if end is not None else n - 1

    def block_memory(_v: int, cumsum: list = (0, 0, 0), from_left=False) -> int:
        """Calculate memory of a block from the cumulative sum of block[v+1, w] or block[b, v-1]"""
        cumsum[MEM] += memory[_v]
        return cumsum[MEM]

    def block_cpu(_v: int, cumsum: list = (0, 0, 0, (0, 1)), from_left=False) -> int:
        """Calculate CPU need of a block from the cached cumulative sum of block[v+1, w]"""
        cumsum[CPU][0] = max(cumsum[CPU][0], rate[_v])
        if from_left:
            return cumsum[CPU][0]
        cumsum[CPU][1] = max(cumsum[CPU][1], math.ceil(cumsum[CPU][0] / rate[_v]))
        return cumsum[CPU][1]

    def block_cost(_v: int, cumsum: list = (0, 0, 0), from_left: bool = False) -> int:
        """Calculate running time of block[b, w] from the cumulative sum of block[v+1, w] or block[0, v-1]"""
        cumsum[COST] += runtime[_v]
        return rate[0 if from_left else _v] * (math.ceil(cumsum[COST] / unit) * unit)

    def block_latency(_b: int, _w: int, cumsum: list = (0, 0, 0), from_left: bool = False) -> int:
        """Calculate relevant latency for block[b, w] from the cumulative sum of block[b+1, w] or block[0, w-1]"""
        # Do not consider latency if no intersection
        if end < _b or _w < start:
            return 0
        if from_left:
            # No need to add next node if it is outside of intersection
            if end < _w:
                return cumsum[LAT]
            cumsum[LAT] += runtime[_w]
        else:
            # No need to add next node if it is outside of intersection
            if b < start:
                return cumsum[LAT]
            cumsum[LAT] += runtime[_b]
        # Ignore delay if latency path starts within the subchain
        return delay + cumsum[LAT] if start < _b else cumsum[LAT]

    # Check lower bound for latency limit
    if L < (lat_min := sum(runtime[start: end + 1])):
        return None, None, lat_min
    # Check if memory constraint allows feasible solutions for the given latency constraint
    k_min = max(math.ceil(sum(memory[start: end + 1]) / M),
                sum(1 for i, j in itertools.pairwise(rate) if math.ceil(j / i) > N))
    k_max = math.floor(min((L - sum(runtime[start: end + 1])) / delay + 1, n))
    if k_max < k_min:
        return None, None, None
    # Check single node partitioning
    if len(runtime) == 1:
        return [0], block_cost(0), block_latency(0, 0)
    # Initialize DP matrix -> DP[i][j][BARR, COST, LAT]
    DP = np.full((n, n, 3), np.inf)
    # Define cache for cumulative block attribute calculations: [MEM, COST, LAT]
    __cache = [0, 0, 0, [0, 0]]
    # Initialize default values for grouping first w nodes into one group
    for w in range(0, n):
        if block_memory(w, __cache, from_left=True) > M or block_cpu(w, __cache, from_left=True) > N:
            break
        DP[w, 0] = np.array((0, block_cost(w, __cache, from_left=True), block_latency(0, w, __cache, from_left=True)))
    # Calculate Dynamic Programming matrix
    for w in range(1, n):
        # Cache for the cumulative sum based calculation of expanding block's memory, runtime and latency values
        __cache = [0, 0, 0, [0, 0]]
        for b in reversed(range(1, w + 1)):
            # As k decreases, bigger blocks [k, w] will continue violating the memory constraint
            if block_memory(b, __cache) > M or block_cpu(b, __cache) > N:
                break
            subcases = DP[b - 1, :b] + np.array((0, block_cost(b, __cache), block_latency(b, w, __cache)))
            subcases[:, BARR] = b
            # Store and overwrite subcases with equal costs (<=) to consider larger blocks for lower latency
            feasible_idx = np.flatnonzero((subcases[:, LAT] <= L) & (subcases[:, COST] <= DP[w, 1:b + 1, COST]))
            DP[w, feasible_idx + 1] = subcases[feasible_idx]
    # Index of optimal cost partition, the fist one if multiple min values exist
    k_opt = np.argmin(DP[n - 1, :, COST])
    _, opt_cost, opt_lat = DP[n - 1, k_opt]
    if opt_cost < np.inf:
        return DP if ret_dp else extract_vec_barr(DP, k_opt), opt_cost, opt_lat
    else:
        return None, np.inf, None


def extract_vec_barr(DP: np.array, k: int) -> list[int]:
    """Extract barrier nodes form DP matrix by iteratively backtracking the minimal cost subcases from *k*"""
    barr = []
    B = DP[..., BARR]
    w = len(B) - 1
    for k in reversed(range(0, k + 1)):
        # The cached b value marks the barrier node of the k. block and refers the subcase => C[b-1,k-1] + c[b,w]
        b = int(B[w, k])
        w = b - 1
        barr.append(b)
    barr.reverse()
    return barr
