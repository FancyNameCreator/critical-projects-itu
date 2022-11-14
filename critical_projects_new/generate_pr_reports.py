import argparse
import os
import time
import pandas as pd


PKG_MANAGERS_LIST = [
    "alire",
    # "cargo",
    "chromebrew",
    "clojars",
    "conan",
    "fpm",
    "homebrew",
    "luarocks",
    "nimble",
    "npm",
    # "ports",
    "rubygems",
    "vcpkg"
]
NUMBER_OF_PROJECTS = 1000


def generate_pr_report(source_file_path, output_file_path, include_n_rows=NUMBER_OF_PROJECTS):
    df = pd.read_csv(source_file_path)
    sorted_df = df.sort_values(by=["PAGE_RANK"], ascending=False).head(include_n_rows)
    sorted_df.to_csv(output_file_path)


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
        print(f"Generating reports for: {pkg_manager}")

        path_to_source_file = os.path.join(args.input_directory, pkg_manager, f"nodes_{pkg_manager}.csv")
        path_to_dest_file = os.path.join(args.output_directory, f"{pkg_manager}_top_{NUMBER_OF_PROJECTS}.csv")

        generation_start_time = time.time()
        generate_pr_report(path_to_source_file, path_to_dest_file)
        print(f"Generation took {round(time.time() - generation_start_time, 4)} seconds.")


if __name__ == '__main__':
    main()