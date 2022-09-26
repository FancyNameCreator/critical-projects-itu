import csv

import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import operator


def create_graph(deps_data_path, pkg_data_path):
    dep_df = pd.read_csv(deps_data_path)
    dependency_relations = dep_df[["pkg_idx", "target_idx"]]

    pkg_df = pd.read_csv(pkg_data_path)
    package_names = pkg_df[["idx", "name"]]

    dependency_relation_graph = nx.from_pandas_edgelist(dependency_relations, 'pkg_idx', 'target_idx')
    nx.set_node_attributes(dependency_relation_graph, pd.Series(package_names.name, index=package_names.idx).to_dict(), 'pkg_name')

    return dependency_relation_graph


def add_pagerank_calculation_to_graph(graph):
    pr = nx.pagerank(graph)
    nx.set_node_attributes(graph, pr, 'page_rank')

    return graph


def _write_to_csv(rows, header, path_to_file):
    with open(path_to_file, 'w+', encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def export_nodes_to_neo4j_csv(graph, path_to_dest_file):
    node_list = []

    for node in graph.nodes:
        node_info = (node, graph.nodes[node]["pkg_name"], graph.nodes[node]["page_rank"])
        node_list.append(node_info)

    node_list.sort(key=lambda tup: tup[0])

    _write_to_csv(node_list, (":ID", ":PKG_NAME", ":PAGE_RANK"), path_to_dest_file)


def export_edges_to_neo4j_csv(graph, path_to_dest_file):
    _write_to_csv(graph.edges, (":FROM_PKG", ":TO_PKG"), path_to_dest_file)

# nx.draw(dependency_relation_graph, node_size=50)
# plt.show()

# sorted_d = dict(sorted(pr.items(), key=operator.itemgetter(1), reverse=True))
# print(sorted_d)
#
# for item in sorted_d:
#     print(package_names.iloc[item]["name"])


def main():
    DEPS_DATA_PATH = "C:\\DATA\\S T U D I A\\Master\\P3\\ASE\\critical-projects-itu\\data\\input\\conan\\conan_dependencies_05-17-2022.csv"
    PKG_DATA_PATH = "C:\\DATA\\S T U D I A\\Master\\P3\\ASE\\critical-projects-itu\\data\\input\\conan\\conan_packages_05-17-2022.csv"

    PKG_MANAGER = "conan"
    NODES_EXPORT_PATH = f"C:\\DATA\\S T U D I A\\Master\\P3\\ASE\\critical-projects-itu\\data\\processing\\{PKG_MANAGER}\\nodes_{PKG_MANAGER}.csv"
    EDGES_EXPORT_PATH = f"C:\\DATA\\S T U D I A\\Master\\P3\\ASE\\critical-projects-itu\\data\\processing\\{PKG_MANAGER}\\edges_{PKG_MANAGER}.csv"

    graph = create_graph(DEPS_DATA_PATH, PKG_DATA_PATH)
    graph_with_pageranks = add_pagerank_calculation_to_graph(graph)

    export_nodes_to_neo4j_csv(graph_with_pageranks, NODES_EXPORT_PATH)
    export_edges_to_neo4j_csv(graph_with_pageranks, EDGES_EXPORT_PATH)


if __name__ == '__main__':
    main()
