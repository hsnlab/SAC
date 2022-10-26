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

import networkx as nx
from matplotlib import pyplot as plt

from alg.util import path_blocks
from service.common import *


def draw_tree(sg: nx.DiGraph, partition: list = None, draw_weights=False, draw_blocks=False, figsize=None, ax=None,
              **kwargs):
    """Draw tree and given partitioning in a top-down topological structure"""
    if figsize is None:
        d = nx.dag_longest_path_length(sg)
        figsize = (d, d)
    if ax is None:
        plt.figure(figsize=figsize, dpi=300)
        ax = plt.gca()
    colors = itertools.cycle(('red', 'orange', "brown", 'green', "purple", "blue", "black", "magenta"))
    for v in sg.nodes:
        if COLOR in sg.nodes[v]:
            del sg.nodes[v][COLOR]
    sg.nodes[PLATFORM][COLOR] = "gray"
    if partition:
        for node, pred in nx.bfs_predecessors(sg, PLATFORM):
            if COLOR in sg.nodes[node]:
                continue
            blk = 0
            while node not in partition[blk]:
                blk += 1
            color = next(colors)
            if COLOR in sg.nodes[pred]:
                while sg.nodes[pred][COLOR] == color:
                    color = next(colors)
            for n in partition[blk]:
                sg.nodes[n][COLOR] = color
        node_colors = [sg.nodes[n][COLOR] for n in sg.nodes]
    else:
        node_colors = ["tab:gray" if n == PLATFORM else "tab:green" for n in sg.nodes]
    if draw_weights:
        labels = {n: f"T{sg.nodes[n][RUNTIME]}\nM{sg.nodes[n][MEMORY]}" for n in sg.nodes if n != PLATFORM}
    else:
        labels = {n: n for n in sg.nodes}
    labels[PLATFORM] = PLATFORM
    pos = nx.drawing.nx_agraph.graphviz_layout(sg, prog='dot', root=0)
    nx.draw(sg, ax=ax, pos=pos, arrows=True, arrowsize=20, width=2, with_labels=True, node_size=1000, font_size=10,
            font_color="white", labels=labels, node_color=node_colors, **kwargs)
    if draw_weights:
        nx.draw_networkx_edge_labels(sg, pos, edge_labels=nx.get_edge_attributes(sg, RATE), label_pos=0.5,
                                     font_size=10)
    if draw_blocks:
        dist_x = sorted({k[0] for k in pos.values()})
        dist_y = sorted({k[1] for k in pos.values()})
        off_x = 0.5 * (sum(abs(b - a) for a, b in itertools.pairwise(dist_x)) // len(dist_x)
                       if len(dist_x) > 1 else pos[PLATFORM][0])
        off_y = 0.5 * (sum(abs(b - a) for a, b in itertools.pairwise(dist_y)) // len(dist_y)
                       if len(dist_y) > 1 else pos[PLATFORM][1])
        for blk in partition:
            lefts, rights = [(pos[blk[0]][0] - off_x, pos[blk[0]][1] + off_y)], \
                            [(pos[blk[0]][0] + off_x, pos[blk[0]][1] + off_y)]
            levels = path_blocks(list(nx.topological_generations(sg)), blk)
            for i, lvl in enumerate(levels):
                lefts.append((pos[min(lvl)][0] - off_x, pos[min(lvl)][1]))
                rights.append((pos[max(lvl)][0] + off_x, pos[max(lvl)][1]))
            lefts.append((pos[min(levels[-1])][0] - off_x, pos[min(levels[-1])][1] - off_y))
            rights.append((pos[max(levels[-1])][0] + off_x, pos[max(levels[-1])][1] - off_y))
            rights.reverse()
            poly = plt.Polygon(lefts + rights, closed=True, fc=sg.nodes[blk[0]][COLOR], ec=sg.nodes[blk[0]][COLOR],
                               lw=3, ls=':', fill=True, alpha=0.3, capstyle='round', zorder=0)
            ax.add_patch(poly)
    plt.title(sg.graph[NAME])
    plt.tight_layout()
    plt.show()
    plt.close()