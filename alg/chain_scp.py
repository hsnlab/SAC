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
import functools
import itertools
import math
import typing


class State(typing.NamedTuple):
    """Store block attributes for a given subcase"""
    barr: int = None  # Barrier/heading node of the last block in the given subcase partitioning
    cost: int = math.inf  # Sum cost of the partitioning
    lat: int = math.inf  # Sum latency of the partitioning regarding the limited subchain[start, end]

    def __repr__(self):
        return repr(tuple(self))


def chain_partitioning(runtime: list, memory: list, rate: list, M: int = math.inf, N: int = math.inf,
                       L: int = math.inf, start: int = 0, end: int = None, delay: int = 1,
                       unit: int = 100, ret_dp: bool = False) -> tuple[list, int, int]:
    """
    Calculates minimal-cost partitioning of a chain based on the node properties of *running time*, *memory usage* and
    *invocation rate* with respect to an upper bound **M** on the total memory of blocks and a latency constraint **L**
    defined on the subchain between *start* and *end* nodes.

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

    @functools.lru_cache(maxsize=n - 1)
    def block_memory(_b: int, _w: int) -> int:
        """Calculate memory of block[b, w]"""
        return sum(memory[_b: _w + 1])

    @functools.lru_cache(maxsize=n - 1)
    def block_cpu(_b: int, _w: int) -> int:
        """Calculate memory of block[b, w]"""
        r_max = itertools.chain((1,), enumerate(itertools.accumulate(reversed(rate[_b: _w + 1]), max)))
        return functools.reduce(lambda pre, max_i: max(pre, math.ceil(max_i[1] / rate[_w - max_i[0]])), r_max)

    @functools.lru_cache(maxsize=n - 1)
    def block_cost(_b: int, _w: int) -> int:
        """Calculate running time of block[b, w]"""
        return rate[_b] * (math.ceil(sum(runtime[_b: _w + 1]) / unit) * unit)

    @functools.lru_cache(maxsize=n - 1)
    def block_latency(_b: int, _w: int) -> int:
        """Calculate relevant latency for block[b, w]"""
        # Do not consider latency if no intersection
        if end < _b or _w < start:
            return 0
        blk_lat = sum(runtime[max(_b, start): min(_w, end) + 1])
        # Ignore delay if latency path starts within the subchain
        return delay + blk_lat if start < _b else blk_lat

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
        return [0], block_cost(0, 0), block_latency(0, 0)
    # Initialize left triangular part of DP matrix -> DP[i][j][COST, LAT, BARR]
    DP = [[State() for _ in range(i + 1)] for i in range(n)]
    # Initialize default values for grouping first w nodes into one group
    for w in range(0, n):
        if block_memory(0, w) > M or block_cpu(0, w) > N:
            break
        DP[w][0] = State(0, block_cost(0, w), block_latency(0, w))
    # Calculate Dynamic Programming matrix
    for w in range(1, n):
        for k in range(1, w + 1):
            for b in reversed(range(k, w + 1)):
                # As k decreases, bigger blocks [k, w] will continue violating the memory constraint
                if block_memory(b, w) > M or block_cpu(b, w) > N:
                    break
                if (lat := DP[b - 1][k - 1].lat + block_latency(b, w)) <= L:
                    # Store and overwrite subcases with equal costs (<=) to consider larger blocks for lower latency
                    if (cost := DP[b - 1][k - 1].cost + block_cost(b, w)) <= DP[w][k].cost:
                        DP[w][k] = State(b, cost, lat)
            # If first w node cannot be partitioned into k blocks due to L then it cannot be partitioned into k+1
            if DP[w][k - 1].lat < DP[w][k].lat == math.inf:
                break
    # Index of optimal cost partition, the fist one if multiple min values exist
    k_opt = min(range(n), key=lambda x: DP[-1][x].cost)
    _, opt_cost, opt_lat = DP[-1][k_opt]
    if opt_cost < math.inf:
        return DP if ret_dp else extract_barr(DP, k_opt), opt_cost, opt_lat
    else:
        return None, math.inf, None


def extract_barr(DP: list[list[State]], k: int) -> list[int]:
    """Extract barrier nodes form DP matrix by iteratively backtracking the minimal cost subcases from *k*"""
    barr = []
    w = len(DP) - 1
    for k in reversed(range(0, k + 1)):
        # The cached b value marks the barrier node of the k. block and refers the subcase => C[b-1,k-1] + c[b,w]
        b = DP[w][k].barr
        w = b - 1
        barr.append(b)
    barr.reverse()
    return barr
