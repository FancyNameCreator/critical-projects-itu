"""
Call me like:
python -m critical_projects_old.projects_to_neo4j_csv data/input/projects-1.6.0-2020-01-12.csv > data/processing/projects_neo4j.csv


The argument to this script has to be a path the the projects CSV file, e.g., 
`projects-1.6.0-2020-01-12.csv` from the libraries.io dataset.

See:
https://libraries.io/data
https://zenodo.org/record/3626071/files/libraries-1.6.0-2020-01-12.tar.gz

The description of the fields can be found here:
https://libraries.io/data#projectFields
"""


import os
import csv
import sys
import time
from critical_projects_old import INCLUDED_PLATFORMS


def main(to_path_file, from_path_file):

    header_line = (":ID", ":LABEL", "Name", "CreatedTS", "RepoURL", "VersionsCount", "SourceRank", "DependentProjectsCount", "Language", "Status", "DependentRepositoriesCount")

    with open(to_path_file, "w+", encoding="utf-8") as fdest:
        csv_writer = csv.writer(fdest)
        csv_writer.writerow(header_line)

        with open(from_path_file, "r", encoding="utf-8") as fp:
            csv_reader = csv.reader(fp, delimiter=",")
            next(csv_reader)  # Skip the header line
            for row in csv_reader:
                #      ['ID', 'Platform', 'Name', 'Created Timestamp', 'Updated Timestamp',
                # 'Description', 'Keywords', 'Homepage URL', 'Licenses', 'Repository URL',
                # 'Versions Count', 'SourceRank', 'Latest Release Publish Timestamp',
                # 'Latest Release Number', 'Package Manager ID',
                # 'Dependent Projects Count', 'Language', 'Status',
                # 'Last synced Timestamp', 'Dependent Repositories Count',
                # 'Repository ID']
                (
                    iD,
                    platform,
                    name,
                    created_timestamp,
                    _,
                    _,
                    _,
                    _,
                    _,
                    repository_url,
                    versions_count,
                    sourcerank,
                    _,
                    _,
                    _,
                    dependent_projects_count,
                    language,
                    status,
                    _,
                    dependent_repositories_count,
                    _,
                ) = row
                if platform in INCLUDED_PLATFORMS:
                    csv_writer.writerow(
                        (
                            iD,
                            platform,
                            name,
                            created_timestamp,
                            repository_url,
                            versions_count,
                            sourcerank,
                            dependent_projects_count,
                            language,
                            status,
                            dependent_repositories_count,
                        )
                    )


if __name__ == "__main__":
    source_csv_file = sys.argv[1]
    dest_csv_file = sys.argv[2]

    print(source_csv_file, dest_csv_file)
    time.sleep(2)

    if os.path.isfile(source_csv_file):
        main(from_path_file=source_csv_file, to_path_file=dest_csv_file)
    else:
        print(__doc__)
