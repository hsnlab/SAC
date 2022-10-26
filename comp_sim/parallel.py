#!/usr/bin/env python3.10
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
import multiprocessing
import os
import random
import time

from func import test_function, STD


def create_service(partition: list[list[tuple[int, int, int]]]) -> list:
    """Generate groups of functions with randomized arguments"""
    return [[functools.partial(test_function, f"FN{num}", runtime, rate) for (num, runtime, rate) in group]
            for group in partition]


def create_random_service(n: int, c: int, runtime=(10, 100), rate=(1, 3)) -> list:
    """Generate randomized service parameters based on a randomized partition of *n* function with *c* cuts"""
    barriers = [0] + sorted(random.sample(population=range(1, n), k=c - 1))
    partition = [[(f, random.randint(*runtime), random.randint(*rate)) for f in range(b, w)]
                 for b, w in itertools.pairwise(barriers + [n])]
    return create_service(partition=partition)


########################################################################################################################

def execute_group(name: str, funcs: list, param: int, cpu: int = os.cpu_count()) -> list[int]:
    """Execute one group of functions in a fully-parallelized manner"""
    print(f"GROUP({name}) execution initiated at {time.time()}")
    data = [param]
    for func in funcs:
        with multiprocessing.Pool(cpu) as pool:
            output = pool.map(func=func, iterable=data)
        data = list(itertools.chain.from_iterable(output))
    print(f"GROUP({name}) execution ended at {time.time()}")
    return data


def execute_service_path(partition: list[list[int]], delay: int, init_data: int) -> int:
    """Execute given service as one path without the cloud-platform parallelization"""
    print(f"SERVICE execution initiated at {time.time()} with input: {init_data}")
    data = init_data
    for i, group in enumerate(partition):
        # Platform invocation delay
        time.sleep(delay / 1000)
        output = execute_group(name=f"GP{i}", funcs=group, param=data)
        # Only choose one data from the output for the simulation
        data = output[random.randint(0, len(output) - 1)]
    print(f"SERVICE execution ended at {time.time()} with output: {data}")
    return data


def simulate(service: list[list], inv_delay: int = 10, input_data: int = 42) -> int:
    print(f">>> Start simulation")
    t_start = time.time()
    output = execute_service_path(partition=service, delay=inv_delay, init_data=input_data)
    t_end = time.time()
    print(f"\n>>> Sum simulation time: {1000 * (t_end - t_start)} ms")
    return output


########################################################################################################################

def simulate_random(n: int = 10, c: int = 3, t=(10, 100), r=(1, 3), d: int = 10, data: int = 42) -> int:
    """Simulate the execution of one random service of *n* functions"""
    rand_service = create_random_service(n=n, c=c, runtime=t, rate=r)
    simulate(service=rand_service, inv_delay=d, input_data=data)


def simulated_test1():
    # --> [Fn1] -1-> [Fn2] -2-> [Fn3] -1-> [Fn4] -2-> [Fn5] -2-> [Fn6] -3-> [Fn7] -2-> [Fn8] -1-> [Fn9] -2-> [Fn10] -->
    # P = [ [1, 2, 3],  [4, 5],  [6],  [7, 8, 9, 10] ]
    inv_delay = 10
    input_data = 42
    partition = [[(1, 40, 1), (2, 60, 2), (3, 50, 1)],
                 [(4, 80, 2), (5, 110, 2)],
                 [(6, 70, 3)],
                 [(7, 50, 2), (8, 30, 1), (9, 90, 2), (10, 40, 1)]]
    service = create_service(partition=partition)
    simulate(service=service, inv_delay=inv_delay, input_data=input_data)
    ###
    block_times = [sum(f_time for (_, f_time, _) in block) for block in partition]
    exec_time = sum(block_times) + inv_delay * (len(block_times) - 1)
    sum_std = STD * 10
    print(f">>> Est. execution time: {exec_time} +/- {sum_std} ms", )


if __name__ == '__main__':
    # simulate_random()
    simulated_test1()
