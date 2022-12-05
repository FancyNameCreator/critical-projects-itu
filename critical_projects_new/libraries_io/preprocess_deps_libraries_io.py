"""
Call me like:
python -m critical_projects.deps_to_neo4j_csv data/input/dependencies-1.6.0-2020-01-12.csv > data/processing/deps_neo4j.csv


The argument to this script has to be a path the the dependiencies CSV file, e.g., 
`dependencies-1.6.0-2020-01-12.csv` from the libraries.io dataset.

See:
https://libraries.io/data
https://zenodo.org/record/3626071/files/libraries-1.6.0-2020-01-12.tar.gz

The description of the fields can be found here:
https://libraries.io/data#dependenciesFields
"""


import os
import csv
import sys
import time

from critical_projects import INCLUDED_PLATFORMS


def main_dependencies(source_file_path, dest_file_path, package_manager):

    header_line = ("pkg_idx", "target_idx", "pkgman")

    os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
    with open(dest_file_path, "w+", encoding="utf-8") as fdest:
        csv_writer = csv.writer(fdest)
        csv_writer.writerow(header_line)

        with open(source_file_path, "r", encoding="utf-8") as fp:
            csv_reader = csv.reader(fp, delimiter=",")
            next(csv_reader)  # Skip the header line
            for row in csv_reader:
                # ID,Platform,Project Name,Project ID,Version Number,Version ID,
                # Dependency Name,Dependency Platform,Dependency Kind,
                # Optional Dependency,Dependency Requirements,Dependency Project ID
                (
                    _,
                    platform,
                    _,
                    project_id,
                    _,
                    _,
                    _,
                    dep_platform,
                    _,
                    _,  # Do I need info on optionality for something?
                    _,
                    dependency_project_id,
                ) = row
                if (
                    dependency_project_id
                    and platform in INCLUDED_PLATFORMS
                    and dep_platform in INCLUDED_PLATFORMS
                    and platform == dep_platform
                    and platform == package_manager
                ):
                    csv_writer.writerow(
                        (
                            project_id,
                            dependency_project_id,
                            platform.lower()
                        )
                    )

        # INFO: dependency_project_id can be empty. In that case the dependency is
        # not known as a project. We could just keep these relations and let Neo4j
        # drop them on import, but to save disk space we remove them here directly.
        # Another way to handle these is to create a second node dataset for all of
        # these projects. We do not do this for this study.


if __name__ == "__main__":
    source_csv_file = sys.argv[1]
    dest_csv_file = sys.argv[2]

    print(source_csv_file, dest_csv_file)
    time.sleep(2)

    if os.path.isfile(source_csv_file):
        main_dependencies(source_csv_file, dest_csv_file)
    else:
        print(__doc__)
