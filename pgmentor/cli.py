import argparse
from pgmentor.configurator import section_host_os, section_pg_params, run_all_sections
from pgmentor.metrics import gather_metrics
from pgmentor.db import Pg

def main():
    parser = argparse.ArgumentParser(prog="pgmentor", description="pgmentor is a tool that provides recommendations for query optimization and PostgreSQL configuration.")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    parser.add_argument("--configure", action="store_true", help="Show PostgreSQL configuration recommendations")
    parser.add_argument("--conninfo", help="Connection string to PostgreSQL server")
    parser.add_argument("--out-file", dest="out_file", help="Write ALTER SYSTEM recommendations to SQL file")
    parser.add_argument("-p", dest="profile", choices=["oltp", "olap"], default="oltp", help="Profile to use")
    args = parser.parse_args()

    if args.configure:
        with Pg(args.conninfo) as pg:
            m = gather_metrics(pg)
            section_host_os(pg, args.profile, m)
            section_pg_params(pg, m, args.profile, args.out_file)
            run_all_sections(pg)
    
    return 0


if __name__ == "__main__":
    main()