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
import networkx as nx

from misc.plot import draw_tree
from service.common import PLATFORM, RUNTIME, MEMORY, RATE

DAY = "DAYTIME"
NIGHT = "NIGHTTIME"


def generate_daytime_service(save: bool = False) -> nx.DiGraph:
    service = nx.DiGraph(name=DAY)
    service.add_nodes_from([PLATFORM,
                            (1, {RUNTIME: 288, MEMORY: 226}),  # IMAGE_REGISTRATION
                            (2, {RUNTIME: 434, MEMORY: 349}),  # IMAGE_TRANSFORMATION
                            (3, {RUNTIME: 1928, MEMORY: 1635}),  # CAR_DETECTION
                            (4, {RUNTIME: 4, MEMORY: 82}),  # DISPLAY_UPDATE
                            (5, {RUNTIME: 121, MEMORY: 110}),  # FREE_SLOT_DETECTION
                            (6, {RUNTIME: 4, MEMORY: 75}),  # REGISTER_UPDATE
                            (7, {RUNTIME: 4, MEMORY: 89}),  # LOGGING
                            (8, {RUNTIME: 21, MEMORY: 110}),  # SLOT_VALIDATION
                            (9, {RUNTIME: 4, MEMORY: 77}),  # QUEUE_WARNING
                            (10, {RUNTIME: 170, MEMORY: 193}),  # CAR_CUT
                            (11, {RUNTIME: 45, MEMORY: 261}),  # ANONYMIZED_STATISTICS
                            (12, {RUNTIME: 378, MEMORY: 216}),  # PLATE_RECOGNITION
                            (13, {RUNTIME: 21, MEMORY: 73}),  # LICENSE_VALIDATION
                            (14, {RUNTIME: 4, MEMORY: 80}),  # SYSTEM_UPDATE
                            ])
    service.add_weighted_edges_from([(PLATFORM, 1, 1),  # camera ID
                                     (1, 2, 1),  # image ID
                                     (2, 3, 1),  # image ID
                                     (3, 4, 1),  # #cars
                                     (3, 5, 1),  # mask ID
                                     (3, 8, 9),  # car coordinates
                                     (5, 6, 3),  # spot
                                     (5, 7, 3),  # spot, log
                                     (8, 9, 2),  # in-queue cars
                                     (8, 10, 7),  # parked cars
                                     (10, 11, 7),  # car crop ID
                                     (10, 12, 7),  # car crop ID
                                     (12, 13, 7),  # license plate
                                     (13, 14, 7),  # result
                                     ], weight=RATE)
    if save:
        nx.write_gml(service, "service_daytime.gml")
    else:
        return service


def generate_nighttime_service(save: bool = False) -> nx.DiGraph:
    service = nx.DiGraph(name=NIGHT)
    service.add_nodes_from([PLATFORM,
                            (1, {RUNTIME: 289, MEMORY: 226}),  # IMAGE_REGISTRATION
                            (2, {RUNTIME: 437, MEMORY: 349}),  # IMAGE_TRANSFORMATION
                            (3, {RUNTIME: 1928, MEMORY: 1635}),  # CAR_DETECTION
                            (4, {RUNTIME: 4, MEMORY: 82}),  # DISPLAY_UPDATE
                            (5, {RUNTIME: 279, MEMORY: 111}),  # FREE_SLOT_DETECTION
                            (6, {RUNTIME: 4, MEMORY: 75}),  # REGISTER_UPDATE
                            (7, {RUNTIME: 4, MEMORY: 89}),  # LOGGING
                            (8, {RUNTIME: 28, MEMORY: 111}),  # SLOT_VALIDATION
                            (9, {RUNTIME: 4, MEMORY: 77}),  # QUEUE_WARNING
                            (10, {RUNTIME: 172, MEMORY: 193}),  # CAR_CUT
                            (11, {RUNTIME: 48, MEMORY: 261}),  # ANONYMIZED_STATISTICS
                            (12, {RUNTIME: 376, MEMORY: 216}),  # PLATE_RECOGNITION
                            (13, {RUNTIME: 27, MEMORY: 74}),  # LICENSE_VALIDATION
                            (14, {RUNTIME: 4, MEMORY: 77}),  # SYSTEM_UPDATE
                            ])
    service.add_weighted_edges_from([(PLATFORM, 1, 1),  # camera ID
                                     (1, 2, 1),  # image ID
                                     (2, 3, 1),  # image ID
                                     (3, 4, 1),  # #cars
                                     (3, 5, 1),  # mask ID
                                     (3, 8, 2),  # car coordinates
                                     (5, 6, 8),  # spot
                                     (5, 7, 8),  # spot, log
                                     (8, 9, 1),  # in-queue cars
                                     (8, 10, 2),  # parked cars
                                     (10, 11, 2),  # car crop ID
                                     (10, 12, 2),  # car crop ID
                                     (12, 13, 2),  # license plate
                                     (13, 14, 2),  # result
                                     ], weight=RATE)
    if save:
        nx.write_gml(service, "service_nighttime.gml")
    else:
        return service


def plot_daytime():
    tree = generate_daytime_service()
    draw_tree(tree, draw_weights=False)


def plot_nighttime():
    tree = generate_nighttime_service()
    draw_tree(tree, draw_weights=False)


if __name__ == '__main__':
    # generate_daytime_service(save=True)
    # generate_nighttime_service(save=True)
    plot_daytime()
    plot_nighttime()
