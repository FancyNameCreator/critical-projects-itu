import csv
import networkx as nx
import pandas as pd
import os
import threading
import time


PKG_MANAGERS_LIST = [
    # "alire",
    # "cargo",
    # "chromebrew",
    # "clojars",
    # "conan",
    # "fpm",
    # "homebrew",
    # "luarocks",
    # "nimble",
    "npm",
    # "ports",
    # "rubygems",
    # "vcpkg"
]

# BASE_PATH = "E:\\Studia - Szymon\\critical-projects-itu"
BASE_PATH = "C:\\DATA\\S T U D I A\\Master\\P3\\ASE\\critical-projects-itu"


def generate_csvs(pkg_manager, deps_data_path, pkg_data_path, export_directory_path, nodes_export_file_name, edges_export_file_name):
    if not os.path.exists(export_directory_path):
        os.makedirs(export_directory_path)

    graph = create_graph(deps_data_path, pkg_data_path)
    graph_with_pageranks = add_pagerank_calculation_to_graph(graph)

    export_nodes_to_neo4j_csv(graph_with_pageranks, pkg_manager, export_directory_path, nodes_export_file_name)
    export_edges_to_neo4j_csv(graph_with_pageranks, pkg_manager, export_directory_path, edges_export_file_name)


def create_graph(deps_data_path, pkg_data_path):
    dependency_relations, package_names = load_and_preprocess_data(deps_data_path, pkg_data_path)

    print(f"{threading.current_thread().name}: Creating graph... ")
    dependency_relation_graph = nx.from_pandas_edgelist(dependency_relations, 'pkg_idx', 'target_idx', edge_attr=['kind'])

    nx.set_node_attributes(dependency_relation_graph, pd.Series(package_names.name, index=package_names.idx).to_dict(), 'pkg_name')
    nx.set_node_attributes(dependency_relation_graph, pd.Series(package_names.pkgman, index=package_names.idx).to_dict(), 'pkg_manager')

    return dependency_relation_graph


def load_and_preprocess_data(deps_data_path, pkg_data_path):
    print(f"{threading.current_thread().name}: Loading data... ")
    dep_df = pd.read_csv(
        filepath_or_buffer=deps_data_path,
        usecols=["pkg_idx", "target_idx", "target_name", "kind"],
    )
    pkg_df = pd.read_csv(
        filepath_or_buffer=pkg_data_path,
        usecols=["idx", "name", "pkgman"],
    )

    return preprocess_dependency_relations(dep_df), preprocess_packages(pkg_df)


def preprocess_dependency_relations(dep_df):
    print(f"{threading.current_thread().name}: Preprocessing dependency relations... ")

    dep_df['kind'].fillna("unknown", inplace=True)
    dep_df.dropna(inplace=True)

    dep_df.target_name = dep_df.target_name.astype('category')
    dep_df.kind = dep_df.kind.astype('category')
    dep_df.pkg_idx = dep_df.pkg_idx.astype(int)
    dep_df.target_idx = dep_df.target_idx.astype(int)

    return dep_df


def preprocess_packages(pkg_df):
    print(f"{threading.current_thread().name}: Preprocessing packages... ")

    pkg_df.name = pkg_df.name.astype('category')
    pkg_df.pkgman = pkg_df.pkgman.astype('category')
    pkg_df.idx = pkg_df.idx.astype(int)

    return pkg_df


def add_pagerank_calculation_to_graph(graph):
    print(f"{threading.current_thread().name}: Computing pagerank... ")
    nx.set_node_attributes(graph, nx.pagerank(graph), 'page_rank')

    return graph


def export_nodes_to_neo4j_csv(graph, pkg_manager, export_directory_path, edges_export_file_name):
    print(f"{threading.current_thread().name}: Exporting nodes... ")
    node_list = [
        (f"{pkg_manager}{str(node)}", graph.nodes[node]["pkg_name"], graph.nodes[node]["page_rank"], graph.nodes[node]["pkg_manager"])
        for node in graph.nodes
    ]

    _write_to_csv(node_list, (":ID", "PKG_NAME", "PAGE_RANK", ":LABEL"), export_directory_path, edges_export_file_name)


def export_edges_to_neo4j_csv(graph, pkg_manager, export_directory_path, nodes_export_file_name):
    print(f"{threading.current_thread().name}: Exporting edges... ")

    attributes = nx.get_edge_attributes(graph, 'kind')
    edge_list = [
        (f"{pkg_manager}{str(from_id)}", f"{pkg_manager}{str(to_id)}", str(attributes[from_id, to_id]).lower())
        for from_id, to_id in graph.edges
    ]

    _write_to_csv(edge_list, (":START_ID", ":END_ID", ":TYPE"), export_directory_path, nodes_export_file_name)


def _write_to_csv(rows, header, directory_path, file_name):
    with open(f"{directory_path}\\{file_name}", 'w+', encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def main():
    # TODO: Investigate NaNs in the labels in some exports
    # TODO: Make package managers lower case words

    for pkg_manager in PKG_MANAGERS_LIST:
        print("\n=============================================")
        print(f"{threading.current_thread().name}: Generating CSVs for: {pkg_manager}")

        deps_data_path = f"{BASE_PATH}\\data\\input\\{pkg_manager}\\{pkg_manager}_dependencies_05-17-2022.csv"
        pkg_data_path = f"{BASE_PATH}\\data\\input\\{pkg_manager}\\{pkg_manager}_packages_05-17-2022.csv"

        export_directory = f"{BASE_PATH}\\data\\processing\\{pkg_manager}"
        nodes_export_file_name = f"nodes_{pkg_manager}.csv"
        edges_export_file_name = f"edges_{pkg_manager}.csv"

        generation_start_time = time.time()
        generate_csvs(
            pkg_manager,
            deps_data_path,
            pkg_data_path,
            export_directory,
            nodes_export_file_name,
            edges_export_file_name
        )
        print(f"Generation took {round(time.time() - generation_start_time, 4)} seconds.")


if __name__ == '__main__':
    main()
