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


def convert_repository(repo: str):
    if repo.__contains__("github") or repo.__contains__("gitlab"):
        return repo
    else:
        return None


def generate_project_repos(source_file_path, output_file_path, package_mng):
    df = pd.read_csv(
        source_file_path,
        usecols=["name", "repository"],
        converters={"repository": lambda x: convert_repository(x)}
    )
    df.dropna(inplace=True)
    df["pkg_mng"] = package_mng
    df = df.drop_duplicates(subset='name', keep="first")
    df.to_csv(output_file_path, index=False)


def main():
    parser = argparse.ArgumentParser(
        description='Parses the DaSEA dataset CSVs to a list of projects names and repositories.')
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
        print(f"Generating CSVs for: {pkg_manager}")

        path_to_source_file = os.path.join(args.input_directory, pkg_manager, f"{pkg_manager}_versions_05-17-2022.csv")
        path_to_dest_file = os.path.join(args.output_directory, pkg_manager, f"projects_and_repos.csv")

        generation_start_time = time.time()
        generate_project_repos(path_to_source_file, path_to_dest_file, pkg_manager)
        print(f"Generation took {round(time.time() - generation_start_time, 4)} seconds.")


if __name__ == '__main__':
    main()