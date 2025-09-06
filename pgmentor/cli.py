import argparse
from pgmentor.configurator import section_pg_params, run_all_sections
from pgmentor.metrics import gather_metrics
from pgmentor.db import Pg
from pgmentor.analyze_query import analyze_query, analyze_stats

def main():
    parser = argparse.ArgumentParser(prog="pgmentor", description="pgmentor is a tool that provides recommendations for query optimization and PostgreSQL configuration.")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s 0.1.0")
    parser.add_argument("-ci", "--conninfo", help="Connection string to PostgreSQL server")
    parser.add_argument("-o", "--out-file", dest="out_file", help="Write in output file")
    parser.add_argument("-p", "--profile", dest="profile", choices=["oltp", "olap"], default="oltp", help="Profile to use")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--configure", action="store_true", help="Show PostgreSQL configuration recommendations")
    group.add_argument("-q", "--query", help="Query to analyze and optimize")
    group.add_argument("-a", "--analyze-stats", action="store_true", help="Analyze query and show statistics")

    args = parser.parse_args()
    
    with Pg(args.conninfo) as pg:
        if args.configure:
            m = gather_metrics(pg)
            section_pg_params(pg, m, args.profile, args.out_file)
            run_all_sections(pg)
        elif args.query:
            query_analysis_result = analyze_query(pg, args.query)
        elif args.analyze_stats:
            query_analysis_result = analyze_stats(pg)

        if args.query or args.analyze_stats:
            if args.out_file:
                with open(args.out_file, "w") as f:
                    f.write(query_analysis_result)
            else:
                print(query_analysis_result)

    return 0


if __name__ == "__main__":
    main()