import argparse
import csv
import networkx as nx
import pandas as pd
import os
import threading
import time


PKG_MANAGERS_LIST = [
    "alire",
    "cargo",          # something is off with this package manager, do not include it
    "chromebrew",
    "clojars",
    "conan",
    "fpm",
    "homebrew",
    "luarocks",
    "nimble",
    "npm",              # speak with helge about it
    # "ports",
    "rubygems",
    "vcpkg"
]


def generate_csvs(pkg_manager, deps_data_path, pkg_data_path, export_directory_path, nodes_export_file_name, edges_export_file_name):
    if not os.path.exists(export_directory_path):
        os.makedirs(export_directory_path)

    graph = create_graph(deps_data_path, pkg_data_path)
    graph_with_pageranks = add_pagerank_calculation_to_graph(graph)

    export_nodes_to_neo4j_csv(graph_with_pageranks, pkg_manager, export_directory_path, nodes_export_file_name)
    export_edges_to_neo4j_csv(graph_with_pageranks, pkg_manager, export_directory_path, edges_export_file_name)


def create_graph(deps_data_path, pkg_data_path):
    dependency_relations, package_names = load_and_preprocess_data(deps_data_path, pkg_data_path)
    dependency_relation_graph = nx.DiGraph()

    print(f"{threading.current_thread().name}: Creating graph: adding packages... ")
    dependency_relation_graph.add_nodes_from(package_names.idx.values)

    names = pd.Series(package_names.name.values, index=package_names.idx).to_dict()
    nx.set_node_attributes(dependency_relation_graph, names, 'pkg_name')

    pkg_manager = pd.Series(package_names.pkgman.values, index=package_names.idx).to_dict()
    nx.set_node_attributes(dependency_relation_graph, pkg_manager, 'pkg_manager')

    print(f"{threading.current_thread().name}: Creating graph: adding dependency relations... ")
    dependency_relation_graph.add_edges_from(dependency_relations.itertuples(index=False, name=None))

    return dependency_relation_graph


def load_and_preprocess_data(deps_data_path, pkg_data_path):
    print(f"{threading.current_thread().name}: Loading data... ")
    dep_df = pd.read_csv(
        filepath_or_buffer=deps_data_path,
        usecols=["pkg_idx", "target_idx"],
    )
    pkg_df = pd.read_csv(
        filepath_or_buffer=pkg_data_path,
        usecols=["idx", "name", "pkgman"],
    )

    return preprocess_dependency_relations(dep_df), preprocess_packages(pkg_df)


def preprocess_dependency_relations(dep_df):
    print(f"{threading.current_thread().name}: Preprocessing dependency relations... ")

    dep_df.dropna(inplace=True)
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

    nodes_n = graph.number_of_nodes()
    normalized_pageranks = dict(map(lambda item: _normalize_pagerank(item, nodes_n), nx.pagerank(graph).items()))
    nx.set_node_attributes(graph, normalized_pageranks, 'page_rank')

    return graph


def _normalize_pagerank(item, ecosystem_size):
    key = item[0]
    value = item[1]

    size_normalized_value = value / (1/ecosystem_size)
    probabilistic_normalized_value = ((size_normalized_value - 1) / (size_normalized_value + 1)) + 2

    return key, probabilistic_normalized_value


def export_nodes_to_neo4j_csv(graph, pkg_manager, export_directory_path, edges_export_file_name):
    print(f"{threading.current_thread().name}: Exporting nodes... ")

    node_list = [
        (f"{pkg_manager}{str(node)}", graph.nodes[node]["pkg_name"], graph.nodes[node]["page_rank"], str(graph.nodes[node]["pkg_manager"]).lower())
        for node in graph.nodes
    ]

    _write_to_csv(node_list, (":ID", "PKG_NAME", "PAGE_RANK", ":LABEL"), export_directory_path, edges_export_file_name)


def export_edges_to_neo4j_csv(graph, pkg_manager, export_directory_path, nodes_export_file_name):
    print(f"{threading.current_thread().name}: Exporting edges... ")

    edge_list = [
        (f"{pkg_manager}{str(from_id)}", f"{pkg_manager}{str(to_id)}")
        for from_id, to_id in graph.edges
    ]

    _write_to_csv(edge_list, (":START_ID", ":END_ID"), export_directory_path, nodes_export_file_name)


def _write_to_csv(rows, header, directory_path, file_name):
    with open(os.path.join(directory_path, file_name), 'w+', encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description='Parses the DaSEA dataset CSVs to CSVs supported by OSSF tool.')
    parser.add_argument(
        "--input_directory",
        type=str,
        help="Path to input directory containing folders with CSV files.")
    parser.add_argument(
        "--output_directory",
        type=str,
        help="Output directory for CSVs")

    args = parser.parse_args()

    if args.output_directory is None or args.input_directory is None:
        parser.error("Arguments must be specified!")

    for pkg_manager in PKG_MANAGERS_LIST:
        print("\n=============================================")
        print(f"{threading.current_thread().name}: Generating CSVs for: {pkg_manager}")

        deps_data_path = os.path.join(args.input_directory, pkg_manager, f"{pkg_manager}_dependencies_05-17-2022.csv")
        pkg_data_path = os.path.join(args.input_directory, pkg_manager, f"{pkg_manager}_packages_05-17-2022.csv")

        export_directory = os.path.join(args.output_directory, "dasea", pkg_manager)
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
