import argparse
import os
import time
import pandas as pd
import re
import sys


PKG_MANAGERS_LIST = [
    "alire",
    "cargo",
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
        if repo.endswith(".git"):
            repo = re.sub(r'.git$', '', repo)
        if repo.__contains__("/archive/"):
            repo = repo.split("/archive/")[0]
        if repo.__contains__("/releases/"):
            repo = repo.split("/releases/")[0]
        if repo.startswith("git+ssh://git@"):
            return re.sub(r'^git\+ssh://git@', '', repo)
        if repo.startswith("git@github.com:"):
            return repo.replace("git@github.com:", "github.com/")
        if repo.startswith("git+"):
            return re.sub(r'^git\+', '', repo)
        if repo.startswith("git://"):
            return re.sub(r'^git://', '', repo)
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
        path_to_dest_file = os.path.join(args.output_directory, "projects_and_repos", f"{pkg_manager}_projects_and_repos.csv")

        generation_start_time = time.time()
        generate_project_repos(path_to_source_file, path_to_dest_file, pkg_manager)
        print(f"Generation took {round(time.time() - generation_start_time, 4)} seconds.")


if __name__ == '__main__':
    main()