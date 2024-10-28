from collections import deque
import pickle as pkl
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout

def subtree(id, max_distance=5):
    subtree = {}
    queue = deque([(id, 0)])
    visited = set([id])
    while queue:
        current_id, distance = queue.popleft()
        if distance > max_distance:
            break
        if current_id not in subtree:
            subtree[current_id] = gen.get(current_id, [])
        for advisor in gen.get(current_id, []):
            if advisor not in visited:
                visited.add(advisor)
                queue.append((advisor, distance + 1))
    return subtree

with open("/content/gen.pkl","rb") as f:
  gen = pkl.load(f)

id=185866
tree=subtree(id=id,max_distance=10)
G = nx.from_dict_of_lists(tree)

plt.figure(figsize=(10, 8))
pos = nx.nx_pydot.graphviz_layout(G, prog="dot")
nx.draw(G, pos, with_labels=True, node_color='lightblue', node_size=500, font_size=6, font_weight='bold', edge_color='gray', arrowsize=15)
nx.draw_networkx_nodes(G, pos, nodelist=[id], node_color='orange', node_size=500)
nx.draw_networkx_nodes(G, pos, nodelist=[ad for ad in gen[id]], node_color='yellow', node_size=500)
plt.show()
