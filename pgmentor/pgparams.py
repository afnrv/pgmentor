from typing import Any, Optional, List, Tuple
from pgmentor.metrics import Metrics
from pgmentor.db import Pg
from pgmentor.linux_helpers import to_unit

def note(p: str) -> str:
    notes = {
        "shared_buffers": "≈25 % RAM",
        "effective_cache_size": "≈75 % RAM",
        "maintenance_work_mem": "5 % RAM (cap 2 GB)",
        "wal_buffers": "≥16 MB if small",
        "min_wal_size": "ckpt-interval",
        "max_wal_size": "ckpt-interval",
        "work_mem": "1.5×p90 sort",
        "temp_buffers": "≈5 % shared",
        "wal_compression": "compress WAL",
        "wal_writer_delay": "10 ms SSD",
        "wal_keep_size": "2 GB repl lag",
        "synchronous_commit": "remote_write (OLTP)",
        "jit": "OFF (OLTP)",
        "track_io_timing": "IO metrics",
        "log_min_duration_statement": "slow ≥1 s",
        "autovacuum_naptime": "10 s loop",
        "autovacuum_vacuum_cost_delay": "2 ms burst",
    }
    return notes.get(p, "")


def raw_value(p: str, m: Metrics, profile: str, pg: Pg) -> Optional[Any]:
    RAM_MB = m.RAM_MB
    CPU = m.CPU
    SSD = m.SSD
    CKPT_SEC = m.CKPT_SEC
    SORT_MB = m.SORT_MB

    def current_setting(name: str) -> int:
        cur = pg.qval("SELECT setting FROM pg_settings WHERE name=%s;", (name,))
        try:
            return int(cur)
        except Exception:
            return 0

    if p == "max_connections":
        return 200
    if p == "shared_buffers":
        return RAM_MB // 4
    if p == "effective_cache_size":
        return (RAM_MB * 3) // 4
    if p == "maintenance_work_mem":
        return min(2048, RAM_MB // 20)
    if p == "checkpoint_completion_target":
        return 0.9
    if p == "checkpoint_timeout":
        return 900
    if p == "wal_buffers":
        cur = current_setting("wal_buffers")
        mb = cur * 8 // 1024
        v = 16 if mb < 16 else mb
        return v
    if p == "min_wal_size":
        return 2048 if CKPT_SEC > 1800 else 1024
    if p == "max_wal_size":
        return 16384 if CKPT_SEC > 1800 else 8192
    if p == "random_page_cost":
        return 1.1 if SSD else 4
    if p == "effective_io_concurrency":
        return 256 if SSD else 2
    if p == "work_mem":
        return max(4, (SORT_MB * 3) // 2)
    if p == "temp_buffers":
        sb = RAM_MB // 4
        return max(16, sb * 5 // 100)
    if p == "wal_compression":
        return "on"
    if p == "wal_writer_delay":
        return 10
    if p == "wal_keep_size":
        return 2048
    if p in ("max_wal_senders", "max_replication_slots"):
        return 10 if CPU > 10 else CPU
    if p == "synchronous_commit":
        return "remote_write"
    if p == "jit":
        return "on" if profile == "olap" else "off"
    if p == "track_io_timing":
        return "on"
    if p == "log_min_duration_statement":
        return 1000
    if p == "log_checkpoints":
        return "on"
    if p == "log_autovacuum_min_duration":
        return 500
    if p == "autovacuum_naptime":
        return 10
    if p == "autovacuum_vacuum_cost_limit":
        return 2000
    if p == "autovacuum_vacuum_cost_delay":
        return 2
    if p == "autovacuum_max_workers":
        return (CPU // 2) if profile == "olap" else 3
    if p == "max_worker_processes":
        v = 8 if CPU < 8 else CPU
        cur = current_setting("max_worker_processes")
        return max(v, cur)
    if p == "max_parallel_workers":
        v = CPU if CPU < 16 else 16
        cur = current_setting("max_parallel_workers")
        return max(v, cur)
    if p == "max_parallel_workers_per_gather":
        v = (CPU + 1) // 2
        cur = current_setting("max_parallel_workers_per_gather")
        return max(v, cur)
    if p == "max_parallel_maintenance_workers":
        v = CPU if CPU < 4 else 4
        cur = current_setting("max_parallel_maintenance_workers")
        return max(v, cur)
    return None


PARAMS = [
    "max_connections",
    "shared_buffers",
    "effective_cache_size",
    "maintenance_work_mem",
    "checkpoint_completion_target",
    "checkpoint_timeout",
    "wal_buffers",
    "min_wal_size",
    "max_wal_size",
    "random_page_cost",
    "effective_io_concurrency",
    "work_mem",
    "temp_buffers",
    "wal_compression",
    "wal_writer_delay",
    "wal_keep_size",
    "max_wal_senders",
    "max_replication_slots",
    "synchronous_commit",
    "jit",
    "track_io_timing",
    "log_min_duration_statement",
    "log_checkpoints",
    "log_autovacuum_min_duration",
    "autovacuum_naptime",
    "autovacuum_vacuum_cost_limit",
    "autovacuum_vacuum_cost_delay",
    "autovacuum_max_workers",
    "max_worker_processes",
    "max_parallel_workers",
    "max_parallel_workers_per_gather",
    "max_parallel_maintenance_workers",
]


def build_reco(pg: Pg, m: Metrics, profile: str) -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str, str, str]] = [("parameter", "rec", "why", "priority", "speedup")]

    def parse_with_unit(val_str: str, unit: Optional[str]) -> float:
        if val_str is None:
            return 0.0
        try:
            # Numeric fast-path
            return float(val_str)
        except Exception:
            pass
        # Non-numeric or has unit suffix in pg_settings (we'll rely on unit column)
        if not unit:
            return 0.0
        try:
            v = float(val_str)
            return v
        except Exception:
            return 0.0

    def estimate_priority_and_speedup(param: str, cur_str: Optional[str], rec_str: str, unit: Optional[str]) -> Tuple[str, str]:
        # Default
        priority = "low"
        speed = 0

        # Try to get numeric delta where it makes sense
        cur_num = parse_with_unit(cur_str or "0", unit)
        try:
            rec_num = float(rec_str) if rec_str.replace('.', '', 1).isdigit() else cur_num
        except Exception:
            rec_num = cur_num
        delta_ratio = 0.0
        if cur_num > 0:
            try:
                delta_ratio = max(0.0, (rec_num - cur_num) / cur_num)
            except Exception:
                delta_ratio = 0.0

        # Heuristics by parameter
        if param in ("work_mem",):
            if rec_num > cur_num:
                priority = "high" if delta_ratio >= 0.5 else "medium"
                speed = 10 if delta_ratio >= 1.0 else (7 if delta_ratio >= 0.5 else 3)
        elif param in ("shared_buffers", "effective_cache_size"):
            if rec_num > cur_num:
                priority = "medium"
                speed = 5 if delta_ratio >= 0.5 else 2
        elif param in ("random_page_cost",):
            # On SSD lowering cost can help planner
            priority = "medium"
            speed = 3
        elif param in ("effective_io_concurrency",):
            priority = "medium"
            speed = 3
        elif param in ("checkpoint_timeout", "min_wal_size", "max_wal_size", "wal_buffers", "checkpoint_completion_target"):
            priority = "medium"
            speed = 2
        elif param in ("jit",):
            # For OLTP JIT off tends to help latency
            priority = "medium" if profile == "oltp" else "low"
            speed = 2 if profile == "oltp" else 1
        elif param in ("synchronous_commit",):
            # remote_write for OLTP can improve throughput with acceptable durability trade-offs
            priority = "medium"
            speed = 3
        elif param in ("wal_compression",):
            priority = "low"
            speed = 1
        elif param.startswith("autovacuum_") or param.startswith("log_") or param in ("track_io_timing",):
            priority = "low"
            speed = 0

        # Boundaries
        speed = max(0, min(20, int(speed)))
        return priority, f"{speed}%"

    for p in PARAMS:
        v = raw_value(p, m, profile, pg)
        if v is None:
            continue
        unit = pg.qval("SELECT unit FROM pg_settings WHERE name=%s;", (p,))
        cur = pg.qval("SELECT setting FROM pg_settings WHERE name=%s;", (p,))
        if unit:
            try:
                mb = int(v)
                rec_val = str(to_unit(mb, unit))
            except Exception:
                rec_val = str(v)
        else:
            rec_val = str(v)
        priority, speedup = estimate_priority_and_speedup(p, str(cur) if cur is not None else None, rec_val, unit)
        rows.append((p, rec_val, note(p), priority, speedup))
    return rows