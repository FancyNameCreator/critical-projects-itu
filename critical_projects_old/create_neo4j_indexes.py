from critical_projects_old import INCLUDED_PLATFORMS


if __name__ == "__main__":
    for platform in INCLUDED_PLATFORMS:
        print(f"CREATE INDEX FOR (n:{platform}) ON (n.Name);")
