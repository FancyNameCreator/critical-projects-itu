import csv
import networkx as nx
import pandas as pd
import os
import threading


PKG_MANAGERS_LIST = [
    "alire",
    # "cargo",
    # "chromebrew",
    # "clojars",
    # "conan",
    # "fpm",
    # "homebrew",
    # "luarocks",
    # "nimble",
    # "npm",
    # "ports",
    # "rubygems",
    # "vcpkg"
]
BASE_PATH = "E:\\Studia - Szymon\\critical-projects-itu"


def _write_to_csv(rows, header, directory_path, file_name):
    with open(f"{directory_path}\\{file_name}", 'w+', encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def sanitize_input_data(dep_df, pkg_df):
    print(f"{threading.current_thread().name}: Sanitizing input... ")

    sanitized_dependencies = dep_df.copy()
    sanitized_dependencies.dropna(inplace=True)

    # TODO: Looks like dropping NaN has the same effect as checking if package is in package list
    # for index, row in sanitized_dependencies.iterrows():
    #     if row["target_name"] not in pkg_df["name"]:
    #         sanitized_dependencies.drop(index)

    sanitized_dependencies.pkg_idx = sanitized_dependencies.pkg_idx.astype(int)
    sanitized_dependencies.target_idx = sanitized_dependencies.target_idx.astype(int)

    return sanitized_dependencies


def create_graph(deps_data_path, pkg_data_path):
    print(f"{threading.current_thread().name}: Creating graph... ")

    dep_df = pd.read_csv(deps_data_path)
    dependency_relations = dep_df[["pkg_idx", "target_idx", "target_name"]]

    pkg_df = pd.read_csv(pkg_data_path)
    package_names = pkg_df[["idx", "name"]]

    sanitized_dependency_relations = sanitize_input_data(dep_df=dependency_relations, pkg_df=package_names)

    print(f"{threading.current_thread().name}: After sanitizing input... ")
    dependency_relation_graph = nx.from_pandas_edgelist(sanitized_dependency_relations, 'pkg_idx', 'target_idx')
    nx.set_node_attributes(dependency_relation_graph, pd.Series(package_names.name, index=package_names.idx).to_dict(), 'pkg_name')

    return dependency_relation_graph


def add_pagerank_calculation_to_graph(graph):
    print(f"{threading.current_thread().name}: Computing pagerank... ")
    nx.set_node_attributes(graph, nx.pagerank(graph), 'page_rank')

    return graph


def export_nodes_to_neo4j_csv(graph, export_directory_path, edges_export_file_name):
    print(f"{threading.current_thread().name}: Exporting nodes... ")
    node_list = [(node, graph.nodes[node]["pkg_name"], graph.nodes[node]["page_rank"]) for node in graph.nodes]
    _write_to_csv(node_list, (":ID", "PKG_NAME", "PAGE_RANK"), export_directory_path, edges_export_file_name)


def export_edges_to_neo4j_csv(graph, export_directory_path, nodes_export_file_name):
    print(f"{threading.current_thread().name}: Exporting edges... ")
    edge_list = [(from_id, to_id, "runtime") for from_id, to_id in graph.edges]
    _write_to_csv(edge_list, (":START_ID", ":END_ID", ":TYPE"), export_directory_path, nodes_export_file_name)


def generate_csvs(deps_data_path, pkg_data_path, export_directory_path, nodes_export_file_name, edges_export_file_name):
    if not os.path.exists(export_directory_path):
        os.makedirs(export_directory_path)

    graph = create_graph(deps_data_path, pkg_data_path)
    graph_with_pageranks = add_pagerank_calculation_to_graph(graph)

    export_nodes_to_neo4j_csv(graph_with_pageranks, export_directory_path, nodes_export_file_name)
    export_edges_to_neo4j_csv(graph_with_pageranks, export_directory_path, edges_export_file_name)


def main():
    # thread_pool = []

    for pkg_manager in PKG_MANAGERS_LIST:
        print("\n=============================================")
        print(f"Generating CSVs for: {pkg_manager}")

        deps_data_path = f"{BASE_PATH}\\data\\input\\{pkg_manager}\\{pkg_manager}_dependencies_05-17-2022.csv"
        pkg_data_path = f"{BASE_PATH}\\data\\input\\{pkg_manager}\\{pkg_manager}_packages_05-17-2022.csv"

        export_directory = f"{BASE_PATH}\\data\\processing\\{pkg_manager}"
        nodes_export_file_name = f"nodes_{pkg_manager}.csv"
        edges_export_file_name = f"edges_{pkg_manager}.csv"

        generate_csvs(deps_data_path, pkg_data_path, export_directory, nodes_export_file_name, edges_export_file_name)

    #     thread = threading.Thread(
    #         target=generate_csvs,
    #         args=(deps_data_path, pkg_data_path, export_directory, nodes_export_file_name, edges_export_file_name),
    #         name=f"Thread {pkg_manager}"
    #     )
    #     thread_pool.append(thread)
    #     thread.start()
    #
    # for t in thread_pool:
    #     t.join()


if __name__ == '__main__':
    main()
