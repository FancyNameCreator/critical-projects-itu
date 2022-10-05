# import os
# import csv
# from neo4j import GraphDatabase
#
# PKG_MANAGERS_LIST = [
#     "alire",
#     # "cargo",
#     # "chromebrew",
#     # "clojars",
#     # "conan",
#     # "fpm",
#     # "homebrew",
#     # "luarocks",
#     # "nimble",
#     # "npm",
#     # "ports",
#     # "rubygems",
#     # "vcpkg"
# ]
# NUMBER_OF_PROJECTS = 100
#
# uri = "neo4j://localhost:7687"
# driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))
#
#
# def report(tx, pkg_manager, limit):
#     query = f"""MATCH (n:{pkg_manager})
#     RETURN ID(n), n.PKG_NAME, n.PAGE_RANK
#     ORDER BY n.PAGE_RANK DESC
#     LIMIT {limit};"""
#     result = tx.run(query)
#
#     return [
#         (r["ID(n)"], r["n.PKG_NAME"], r["n.PAGE_RANK"])
#         for r in result
#     ]
#
#
# def main():
#     base_path = "C:\\DATA\\S T U D I A\\Master\\P3\\ASE\\critical-projects-itu\\data\\output"
#
#     with driver.session() as session:
#         for pkg_manager in PKG_MANAGERS_LIST:
#             result_str = session.read_transaction(report, pkg_manager, NUMBER_OF_PROJECTS)
#
#             fname = f"{pkg_manager.lower()}_top_{NUMBER_OF_PROJECTS}.csv"
#             outfile = os.path.join(base_path, fname)
#
#             with open(outfile, "w") as fp:
#                 csv_writer = csv.writer(fp)
#                 csv_writer.writerow(("id", "name", "pagerank"))
#                 for r in result_str:
#                     csv_writer.writerow(r)
#
#     driver.close()
#
#
# if __name__ == "__main__":
#     main()
