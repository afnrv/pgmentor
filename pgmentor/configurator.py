from typing import List, Tuple, Dict
from pgmentor.linux_helpers import fmt_bytes, read_first, numa_nodes, numa_bal, calc_need_hp, get_governor, parse_meminfo_kb, get_os, hp_info
from pgmentor.metrics import Metrics
from pgmentor.output import h1, print_kv_table
import shlex
import time
import os
import re
from pgmentor.db import Pg
from pgmentor.pgparams import build_reco
from psycopg2.extras import execute_values


def section_host_os(pg: Pg, profile: str, m: Metrics) -> None:
    h1("0) Host OS parameters")
    # Guard for non-Linux systems
    if not os.path.exists('/proc') or not os.path.exists('/sys'):
        print("Non-Linux platform detected; host OS checks are skipped.")
        print()
        return
    print(f"OS version        : {get_os()}")
    print(f"CPU governor(s)   : {get_governor()}")
    print(f"HugePages status  : {hp_info()}")
    print()

    rows: List[Tuple[str, str, str, str, str]] = []

    def row(param: str, current: str, recommended: str, reason: str, action: str) -> None:
        rows.append((param, current, recommended, reason, action))

    # sysctl
    try:
        cur = int(read_first("/proc/sys/vm/swappiness", "60"))
        row("vm.swappiness", str(cur), "10", "OLTP: ≤10", "sysctl" if cur > 10 else "ok")
    except Exception:
        pass
    try:
        cur = int(read_first("/proc/sys/vm/dirty_ratio", "20"))
        row("vm.dirty_ratio", str(cur), "15", "smoother burst", "sysctl" if cur >= 15 else "ok")
    except Exception:
        pass
    try:
        cur = int(read_first("/proc/sys/vm/dirty_background_ratio", "10"))
        row("vm.dirty_background_ratio", str(cur), "5", "early flush", "sysctl" if cur >= 5 else "ok")
    except Exception:
        pass
    try:
        cur = int(read_first("/proc/sys/vm/overcommit_memory", "0"))
        row("vm.overcommit_memory", str(cur), "2", "strict", "sysctl" if cur != 2 else "ok")
    except Exception:
        pass

    RAM_MB = m.RAM_MB
    need_shm = RAM_MB * 1024 * 1024 // 2
    try:
        cur = int(read_first("/proc/sys/kernel/shmmax", "0"))
        row("kernel.shmmax", fmt_bytes(cur), fmt_bytes(need_shm), "≥ shared_buffers", "sysctl" if cur < need_shm else "ok")
    except Exception:
        pass

    nodes = numa_nodes()
    row("numa_nodes", str(nodes), "≥2 → tune", "how many NUMA nodes", "info")
    nb = numa_bal()
    row("numa_policy", str(nb), "0", "interleave/off", "sysctl" if nb != 0 else "ok")

    thp_path = "/sys/kernel/mm/transparent_hugepage/enabled"
    thp = "n/a"
    if os.path.isfile(thp_path):
        mthp = re.search(r"\[([^\]]+)\]", read_first(thp_path))
        if mthp:
            thp = mthp.group(1)
    row("transparent_hugepage", thp, "disabled", "PG prefers off", "sysfs" if thp != "disabled" else "ok")

    need_hp = calc_need_hp(pg)
    tot = parse_meminfo_kb("HugePages_Total:")
    row("vm.nr_hugepages", str(tot), str(need_hp), "fit shared_buf", "sysctl" if tot < need_hp else "ok")

    row("kernel.numa_balancing", str(nb), "0", "avoid migration", "sysctl" if nb != 0 else "ok")

    gov = get_governor()
    row("cpu_governor", gov, "performance", "latency", "cpufreq" if "performance" not in gov else "ok")

    try:
        ulimit_nofile = int(os.popen("ulimit -n").read().strip() or "1024")
    except Exception:
        ulimit_nofile = 1024
    row("ulimit_nofile", str(ulimit_nofile), "4096", "≥2×conn", "limits" if ulimit_nofile < 4096 else "ok")

    # I/O scheduler & read-ahead
    pgdata = pg.qval("SHOW data_directory;")
    if pgdata:
        try:
            df = os.popen(f"df -P {shlex.quote(pgdata)} | awk 'NR==2{{print $1}}'").read().strip()
            dev = re.sub(r"^/dev/", "", df)
            base = f"/sys/block/{re.sub(r'[0-9]+$', '', dev)}"
            sched_file = os.path.join(base, "queue", "scheduler")
            if os.path.isfile(sched_file):
                cur = read_first(sched_file)
                mcur = re.search(r"\[([^\]]+)\]", cur)
                curv = mcur.group(1) if mcur else cur
                rowsched = (f"{dev}_scheduler", curv, "none", "SSD sched", "sysfs" if curv not in ("none", "mq-deadline") else "ok")
                rows.append(rowsched)
                ra_file = os.path.join(base, "queue", "read_ahead_kb")
                if os.path.isfile(ra_file):
                    ra = read_first(ra_file)
                    rows.append((f"{dev}_read_ahead_kb", ra, "128", "RA 128k", "sysfs" if int(ra) > 512 else "ok"))
        except Exception:
            pass

    print_kv_table(rows)
    print(f"Needed nr_hugepages : {need_hp}  (calc for shared_buffers + 8 MB)")

def section_pg_params(pg: Pg, m: Metrics, profile: str, out_file: str | None) -> None:
    h1("1) PG parameters")
    # build temp table reco
    rows = build_reco(pg, m, profile)
    with pg.conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE reco(parameter text, rec text, why text);")
        execute_values(cur, "INSERT INTO reco(parameter, rec, why) VALUES %s", rows[1:])
        pg.conn.commit()

    sql = """
    WITH diff AS (
      SELECT s.name,
             s.setting::text                        AS cur,
             r.rec                                  AS rec,
             r.why                                  AS why,
             s.context,
             (s.setting::text <> r.rec)::bool       AS differs,
             (s.context = 'postmaster')::bool       AS needs_restart,
             CASE
               WHEN s.context = 'postmaster'                 THEN 'restart'
               WHEN s.context IN ('sighup','superuser')      THEN 'reload'
               WHEN s.context IN ('backend','user')          THEN 'session'
               WHEN s.name LIKE 'autovacuum_%'
                    OR s.name IN ('fillfactor','toast_tuple_target')
                                                    THEN 'table'
               ELSE 'internal'
             END                                            AS scope
      FROM pg_settings s
      JOIN reco r ON r.parameter = s.name
    )
    SELECT name, cur, rec, CASE WHEN differs THEN scope ELSE 'ok' END AS action, why
    FROM diff
    ORDER BY differs DESC, name;
    """
    rows_out = pg.qall(sql)

    tbl: List[Tuple[str, str, str, str, str]] = []
    for name, cur, rec, action, why in rows_out:
        tbl.append((str(name).ljust(32), str(cur).ljust(12), str(rec).ljust(12), action, why))
    print_kv_table(tbl)

    # Write recommended ALTER SYSTEM statements to file if requested
    if out_file:
        stmts: List[str] = []
        # Only include settings that differ from current values
        for name, cur, rec, action, _why in rows_out:
            if action != 'ok':
                # escape single quotes in value
                val = str(rec).replace("'", "''")
                ident = name.replace('"', '""')
                stmts.append(f"ALTER SYSTEM SET \"{ident}\" = '{val}';")
        # Add a helpful comment for reload/restart guidance
        header = [
            "-- pgmentor recommendations",
            "-- Apply at your own discretion. Review each change before running.",
            "-- After execution, run: SELECT pg_reload_conf();  -- or restart if needed",
            ""
        ]
        content = "\n".join(header + stmts) + ("\n" if stmts else "")
        with open(out_file, 'w') as f:
            f.write(content)


def print_query(pg: Pg, sql: str) -> None:
    rows = pg.qall(sql)
    if not rows:
        print("(no rows)")
        return
    # headers from cursor description: re-execute to get description safely
    with pg.conn.cursor() as cur:
        cur.execute(sql)
        desc = [d.name for d in cur.description]
        # compute widths
        width = [max(len(str(x)) for x in [desc[i]] + [r[i] for r in rows]) for i in range(len(desc))]
        # print header
        fmt = " | ".join("{:%d}" % w for w in width)
        print(fmt.format(*desc))
        print("-+-".join("-" * w for w in width))
        for r in rows:
            print(fmt.format(*[str(x) for x in r]))


def run_all_sections(pg: Pg) -> None:
    # Prepare both variants to be resilient across PG versions
    ckpt_sql_checkpointer = """
        SELECT
          num_timed              AS timed_ckpt,
          num_requested          AS req_ckpt,
          round(num_requested*100.0/NULLIF(num_timed+num_requested,0),1) AS "req_%",
          buffers_written        AS buf_ckpt,
          (SELECT buffers_clean FROM pg_stat_bgwriter)         AS buf_bgwriter,
          NULL::bigint           AS buf_backend,
          NULL::bigint           AS backend_fsync,
          (SELECT buffers_alloc FROM pg_stat_bgwriter)         AS buf_alloc
        FROM pg_stat_checkpointer;
    """
    ckpt_sql_bgwriter = """
        SELECT
          NULL::bigint            AS timed_ckpt,
          NULL::bigint            AS req_ckpt,
          NULL::numeric           AS "req_%",
          NULL::bigint            AS buf_ckpt,
          buffers_clean           AS buf_bgwriter,
          buffers_backend         AS buf_backend,
          buffers_backend_fsync   AS backend_fsync,
          buffers_alloc           AS buf_alloc
        FROM pg_stat_bgwriter;
    """

    sections: List[Tuple[str, str]] = [
        ("2) Checkpoint & bgwriter", "__CKPT__"),
        ("3) HOT updates (low %)",
         """
         SELECT schemaname||'.'||relname        AS table,
                n_tup_upd                       AS upd,
                n_tup_hot_upd                   AS hot,
                round(n_tup_hot_upd*100.0/NULLIF(n_tup_upd,0),1) AS hot_pct
         FROM pg_stat_user_tables
         WHERE n_tup_upd > 100
         ORDER BY hot_pct NULLS FIRST
         LIMIT 20;
         """),
        ("4) Seq vs Index scan",
         """
         SELECT schemaname||'.'||relname                          AS table,
                seq_scan, idx_scan,
                round(idx_scan*100.0/NULLIF(seq_scan+idx_scan,0),1) AS idx_pct,
                pg_size_pretty(pg_relation_size(relid))            AS size
         FROM pg_stat_user_tables
         WHERE seq_scan + idx_scan > 0
         ORDER BY seq_scan DESC
         LIMIT 20;
         """),
        ("5) Duplicate indexes",
         """
         WITH sig AS (
           SELECT i.indexrelid,
                  (indrelid::text||':'||indkey::text||':'||
                   COALESCE(indexprs::text,'')||':'||COALESCE(indpred::text,'')) AS signature
           FROM pg_index i
         )
         SELECT pg_size_pretty(SUM(pg_relation_size(indexrelid))) AS dup_size,
                array_agg(indexrelid::regclass)                   AS dup_indexes
         FROM sig
         GROUP BY signature
         HAVING COUNT(*) > 1
         ORDER BY SUM(pg_relation_size(indexrelid)) DESC;
         """),
        ("6) FK without indexes",
         """
         WITH fk AS (
           SELECT conrelid, conname, conkey, confrelid
           FROM pg_constraint WHERE contype='f'
         ), mis AS (
           SELECT fk.conrelid::regclass AS child_table,
                  array_agg(att.attname ORDER BY att.attnum)   AS key_cols,
                  fk.confrelid::regclass AS parent_table,
                  fk.conname             AS fk_name
           FROM fk
           JOIN pg_attribute att ON att.attrelid = fk.conrelid
                                AND att.attnum   = ANY(fk.conkey)
           WHERE NOT EXISTS (
                 SELECT 1 FROM pg_index i
                 WHERE i.indrelid = fk.conrelid
                   AND i.indisvalid
                   AND i.indkey::text = array_to_string(fk.conkey,' ')
           )
           GROUP BY child_table, parent_table, fk_name
         )
         SELECT child_table,
                key_cols,
                parent_table,
                fk_name
         FROM mis
         ORDER BY child_table;
         """),
        ("7) Big tables without PK",
         """
         SELECT c.relname                 AS table,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
                c.reltuples::bigint        AS rows
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind='r'
           AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
           AND pg_total_relation_size(c.oid) > 100*1024*1024
           AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE contype='p' AND conrelid=c.oid)
         ORDER BY pg_total_relation_size(c.oid) DESC
         LIMIT 20;
         """),
        ("8) Unused indexes",
         """
         SELECT schemaname||'.'||relname        AS table,
                indexrelname                    AS index,
                pg_size_pretty(pg_relation_size(indexrelid)) AS size,
                idx_scan
         FROM pg_stat_user_indexes
         JOIN pg_index  USING (indexrelid)
         WHERE idx_scan = 0
           AND indisunique IS FALSE
           AND pg_relation_size(indexrelid) > 10*1024*1024
         ORDER BY pg_relation_size(indexrelid) DESC
         LIMIT 20;
         """),
        ("9) Dead-tuples / bloat",
         """
         SELECT schemaname||'.'||relname                     AS table,
                n_live_tup, n_dead_tup,
                round(n_dead_tup*100.0/NULLIF(n_live_tup+n_dead_tup,0),1) AS dead_pct,
                pg_size_pretty(pg_total_relation_size(relid))             AS total_size
         FROM pg_stat_user_tables
         WHERE n_dead_tup > 0
         ORDER BY dead_pct DESC
         LIMIT 20;
         """),
        ("10) Temp-files usage",
         """
         SELECT datname,
                temp_files,
                pg_size_pretty(temp_bytes) AS temp_bytes
         FROM pg_stat_database
         WHERE temp_files > 0
         ORDER BY temp_bytes DESC
         LIMIT 15;
         """),
        ("11) XID freeze age (databases)",
         """
         WITH cur AS (SELECT txid_current()::bigint AS nowxid)
         SELECT datname,
                age(datfrozenxid)                          AS age_xid,
                2000000000 - age(datfrozenxid)             AS xids_left
         FROM pg_database, cur
         ORDER BY age_xid DESC;
         """),
        ("11) XID freeze age (tables)",
         """
         WITH cur AS (SELECT txid_current()::bigint AS nowxid)
         SELECT s.schemaname||'.'||s.relname         AS table,
                age(c.relfrozenxid)                  AS age_xid,
                2000000000 - age(c.relfrozenxid)     AS xids_left
         FROM pg_stat_user_tables s
         JOIN pg_class c ON c.oid = s.relid
         ORDER BY age_xid DESC
         LIMIT 15;
         """),
        ("12) Wait events snapshot (0.5s)", "SNAP_WAIT"),
        ("13) Replication lag (slots)",
         """
         SELECT slot_name,
                wal_status,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
                active
         FROM pg_replication_slots;
         """),
        ("14) Extensions",
         """
         SELECT e.extname,
                e.extversion,
                n.nspname AS schema
         FROM pg_extension e
         JOIN pg_namespace n ON n.oid = e.extnamespace
         ORDER BY e.extname;
         """),
        ("15) HugePages / Shared memory",
         """
         SELECT name, setting FROM pg_settings
         WHERE name IN ('huge_pages','huge_page_size','shared_memory_type');
         """),
        ("16) Archiving / pg_wal size",
         """
         SELECT setting AS archive_mode      FROM pg_settings WHERE name='archive_mode';
         SELECT setting AS archive_command   FROM pg_settings WHERE name='archive_command';
         SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')) AS current_wal;
         """),
    ]

    for title, sql in sections:
        h1(title)
        if sql == "SNAP_WAIT":
            snap1 = pg.qall("SELECT wait_event_type||':'||wait_event FROM pg_stat_activity WHERE wait_event IS NOT NULL")
            time.sleep(0.5)
            snap2 = pg.qall("SELECT wait_event_type||':'||wait_event FROM pg_stat_activity WHERE wait_event IS NOT NULL")
            counts: Dict[str, int] = {}
            for (e,) in snap1 + snap2:
                counts[e] = counts.get(e, 0) + 1
            print(f"{'event':25} | count")
            print("-" * 27 + "+" + "-" * 7)
            for k, v in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:10]:
                print(f"{k:25} | {v}")
        else:
            if sql == "__CKPT__":
                # Try pg_stat_checkpointer first (PG16+), fall back to bgwriter
                try:
                    print_query(pg, ckpt_sql_checkpointer)
                except Exception:
                    print_query(pg, ckpt_sql_bgwriter)
            else:
                # allow multiple statements separated by ;
                stmts = [s.strip() for s in sql.strip().split(";") if s.strip()]
                for st in stmts:
                    print_query(pg, st + ";")
