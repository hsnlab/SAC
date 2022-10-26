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
from alg.tree_meta_MTP import mtp_tree_partitioning
from alg.tree_rec_BTP import btp_tree_partitioning
from use_case.services import generate_daytime_service, generate_nighttime_service
from misc.util import evaluate_tree_partitioning


def test_daytime_service(tree_partitioning, M: int = 3072, N: int = 3, L: int = 3400, root: int = 1, cp_end: int = 14,
                         delay: int = 80, unit: int = 100):
    service = generate_daytime_service()
    params = dict(sg=service, root=root, M=M, N=N, L=L, cp_end=cp_end, delay=delay, unit=unit)
    partition, opt_cost, opt_lat = tree_partitioning(**params)
    evaluate_tree_partitioning(service, partition, opt_cost, root, cp_end, M, N, L, delay, unit)


def test_nighttime_service(tree_partitioning, M: int = 3072, N: int = 3, L: int = 3400, root: int = 1, cp_end: int = 14,
                           delay: int = 80, unit: int = 100):
    service = generate_nighttime_service()
    params = dict(sg=service, root=root, M=M, N=N, L=L, cp_end=cp_end, delay=delay, unit=unit)
    partition, opt_cost, opt_lat = tree_partitioning(**params)
    evaluate_tree_partitioning(service, partition, opt_cost, root, cp_end, M, N, L, delay, unit)


def test_service_partitioning():
    print("MTP partitioning")
    test_daytime_service(mtp_tree_partitioning)
    test_nighttime_service(mtp_tree_partitioning)
    print("BTP partitioning")
    test_daytime_service(btp_tree_partitioning)
    test_nighttime_service(btp_tree_partitioning)


def test_all_setups():
    params = dict(root=1, M=3072, N=3, L=3400, cp_end=14, delay=80, unit=100)
    day_service = generate_daytime_service()
    night_service = generate_nighttime_service()
    day_part, day_cost, day_lat = btp_tree_partitioning(day_service, **params)
    night_part, night_cost, night_lat = btp_tree_partitioning(night_service, **params)

    print("Daytime service - Daytime partitioning")
    evaluate_tree_partitioning(day_service, day_part, day_cost, **params)
    print("Daytime service - Nighttime partitioning")
    evaluate_tree_partitioning(day_service, night_part, None, **params)
    print("Nighttime service - Nighttime partitioning")
    evaluate_tree_partitioning(night_service, night_part, night_cost, **params)
    print("Nighttime service - Daytime partitioning")
    evaluate_tree_partitioning(night_service, day_part, None, **params)


if __name__ == '__main__':
    # test_service_partitioning()
    test_all_setups()
