import os
import sys
import time

from critical_projects import INCLUDED_PLATFORMS
from preprocess_deps_libraries_io import main_dependencies
from preprocess_projects_libraries_io import main_projects


def export_projects(source_csv_file, dest_dir):
    for platform in INCLUDED_PLATFORMS:
        print(f"Processing: {platform}")
        dest_file = os.path.join(dest_dir, platform.lower(), "projects.csv")
        main_projects(dest_file, source_csv_file, platform)


def export_dependencies(source_csv_file, dest_dir):
    for platform in INCLUDED_PLATFORMS:
        print(f"Processing: {platform}")
        dest_file = os.path.join(dest_dir, platform.lower(), "dependencies.csv")
        main_dependencies(source_csv_file, dest_file, platform)


if __name__ == '__main__':
    source_project_csv_file = sys.argv[1]
    source_dependencies_csv_file = sys.argv[2]
    dest_directory = sys.argv[3]

    # print("PROCESSING PACKAGES:")
    # if os.path.isfile(source_project_csv_file):
    #     export_projects(source_project_csv_file, dest_directory)
    # else:
    #     print(__doc__)

    print("PROCESSING DEPENDENCIES:")
    if os.path.isfile(source_dependencies_csv_file):
        export_dependencies(source_dependencies_csv_file, dest_directory)
    else:
        print(__doc__)