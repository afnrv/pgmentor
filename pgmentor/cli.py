import argparse
from pgmentor.configurator import section_host_os, section_pg_params, run_all_sections
from pgmentor.metrics import gather_metrics
from pgmentor.db import Pg
from pgmentor.analyze_query import analyze_query

def main():
    parser = argparse.ArgumentParser(prog="pgmentor", description="pgmentor is a tool that provides recommendations for query optimization and PostgreSQL configuration.")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    parser.add_argument("--conninfo", help="Connection string to PostgreSQL server")
    parser.add_argument("--out-file", dest="out_file", help="Write in output file")
    parser.add_argument("-p", dest="profile", choices=["oltp", "olap"], default="oltp", help="Profile to use")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--configure", action="store_true", help="Show PostgreSQL configuration recommendations")
    group.add_argument("-q", "--query", help="Query to analyze and optimize")

    args = parser.parse_args()

    if args.configure:
        with Pg(args.conninfo) as pg:
            m = gather_metrics(pg)
            section_host_os(pg, args.profile, m)
            section_pg_params(pg, m, args.profile, args.out_file)
            run_all_sections(pg)
    
    if args.query:
        with Pg(args.conninfo) as pg:
            query_analysis_result = analyze_query(pg, args.query)

            if args.out_file:
                with open(args.out_file, "w") as f:
                    f.write(query_analysis_result)
            else:
                print(query_analysis_result)
    return 0


if __name__ == "__main__":
    main()