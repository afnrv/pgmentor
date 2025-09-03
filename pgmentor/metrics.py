from dataclasses import dataclass
from pgmentor.linux_helpers import parse_meminfo_kb, read_first
from pgmentor.db import Pg
import os
@dataclass
class Metrics:
    RAM_MB: int
    CPU: int
    SSD: bool
    HITR: float
    CKPT_SEC: int
    SORT_MB: int

def gather_metrics(pg: Pg) -> Metrics:
    RAM_MB = parse_meminfo_kb("MemTotal:") // 1024
    CPU = os.cpu_count() or 1
    # Определяем SSD: если найден хоть один rotational==1 → считаем есть HDD
    SSD = True
    try:
        for dev in os.listdir("/sys/block"):
            path = f"/sys/block/{dev}/queue/rotational"
            if os.path.isfile(path):
                val = read_first(path, "0")
                if val.strip() == "1":
                    SSD = False
    except Exception:
        pass

    # HITR (buffer cache hit ratio)
    hitr_val = pg.qval(
        """
        WITH hr AS (
          SELECT 100*sum(blks_hit)/NULLIF(sum(blks_hit)+sum(blks_read),0)::numeric h
          FROM pg_stat_database WHERE blks_hit+blks_read>0)
        SELECT COALESCE(ROUND(h,1),0) FROM hr;
        """
    )
    try:
        hitr = float(hitr_val) if hitr_val is not None else 0.0
    except Exception:
        hitr = 0.0

    # CKPT_SEC (средний интервал между чекпойнтами)
    ckpt_seconds = 0.0
    # PG16+: pg_stat_checkpointer
    try:
        val = pg.qval(
            """
            SELECT COALESCE(EXTRACT(EPOCH FROM now()-stats_reset)/
                   NULLIF(num_timed+num_requested,0),0)
            FROM pg_stat_checkpointer;
            """
        )
        if val is not None:
            ckpt_seconds = float(val)
    except Exception:
        # older PG: pg_stat_bgwriter
        try:
            val = pg.qval(
                """
                SELECT COALESCE(EXTRACT(EPOCH FROM now()-stats_reset)/
                       NULLIF(checkpoints_timed+checkpoints_req,0),0)
                FROM pg_stat_bgwriter;
                """
            )
            if val is not None:
                ckpt_seconds = float(val)
        except Exception:
            ckpt_seconds = 0.0

    CKPT_SEC = int(ckpt_seconds)

    # p90 sort/hash через pg_stat_statements
    SORT_MB = 16
    has_pss = pg.qval("SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements';")
    if has_pss == 1:
        col = pg.qval(
            """
            SELECT CASE
                     WHEN EXISTS (SELECT 1 FROM pg_attribute
                                  WHERE attrelid='public.pg_stat_statements'::regclass
                                    AND attname='total_plan_rows' AND NOT attisdropped)
                       THEN 'total_plan_rows'
                     WHEN EXISTS (SELECT 1 FROM pg_attribute
                                  WHERE attrelid='public.pg_stat_statements'::regclass
                                    AND attname='rows' AND NOT attisdropped)
                       THEN 'rows'
                     ELSE '' END;
            """
        )
        if col:
            q = f"SELECT CEIL(percentile_disc(0.90) WITHIN GROUP (ORDER BY ({col})*8/1024.0)) FROM pg_stat_statements;"
            val = pg.qval(q)
            try:
                SORT_MB = int(val)
            except Exception:
                pass

    return Metrics(RAM_MB=RAM_MB, CPU=CPU, SSD=SSD, HITR=float(hitr), CKPT_SEC=CKPT_SEC, SORT_MB=SORT_MB)