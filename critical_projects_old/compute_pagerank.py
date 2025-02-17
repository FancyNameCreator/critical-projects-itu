from neo4j import GraphDatabase
from critical_projects_old import INCLUDED_PLATFORMS, NUMBER_OF_PROJECTS


uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "6P87!xHs9f!CckgS"))


def create_graph_view(tx, platform):
    graph = f"{platform.lower()}_pr_graph"
    query = f'CALL gds.graph.create("{graph}", "{platform}", "*");\n'
    result = tx.run(query)

    return result


def compute_pagerank(tx, platform):
    graph = f"{platform.lower()}_pr_graph"
    query = (
        f'CALL gds.pageRank.write("{graph}", '
        + '{maxIterations: 20, dampingFactor: 0.85, writeProperty: "pagerank"}'
        + ") YIELD nodePropertiesWritten, ranIterations;"
    )
    result = tx.run(query)

    return result


def main():
    with driver.session() as session:
        for platform in INCLUDED_PLATFORMS:
            print(f"Computing pagerank for {platform}...")
            try:
                r = session.execute_read(create_graph_view, platform)
            except Exception as e:
                print(e)

            try:
                r = session.execute_write(compute_pagerank, platform)
            except Exception as e:
                print(e)

    driver.close()


if __name__ == "__main__":
    main()
