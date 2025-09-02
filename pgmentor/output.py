from typing import List, Tuple, Sequence, Any

def h1(title: str) -> None:
    print("\n" + "=" * 17 + f" {title} " + "=" * 17 + "\n")


def print_kv_table(rows: List[Tuple[str, str, str, str, str]]) -> None:
    # | parameter | current | recommended | action | reason |
    header = ("parameter", "current", "recommended", "action", "reason")
    all_rows = [header] + rows
    widths = [max(len(str(r[i])) for r in all_rows) for i in range(5)]
    def fmt(r: Sequence[Any]) -> str:
        return (
            f"| {str(r[0]).ljust(widths[0])} | "
            f"{str(r[1]).ljust(widths[1])} | "
            f"{str(r[2]).ljust(widths[2])} | "
            f"{str(r[3]).ljust(widths[3])} | "
            f"{str(r[4]).ljust(widths[4])} |"
        )
    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    print(sep)
    print(fmt(header))
    print(sep)
    for r in sorted(rows, key=lambda r: r[0]):
        print(fmt(r))
    print(sep)