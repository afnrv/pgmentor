from typing import List, Tuple, Dict
from pgmentor.metrics import Metrics
from pgmentor.output import h1, print_kv_table
import time
from pgmentor.db import Pg
from pgmentor.pgparams import build_reco
from psycopg2.extras import execute_values


def section_pg_params(pg: Pg, m: Metrics, profile: str, out_file: str | None) -> None:
    h1("1) PG parameters")
    # build temp table reco
    rows = build_reco(pg, m, profile)
    with pg.conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE reco(parameter text, rec text, why text, priority text, speedup text);")
        execute_values(cur, "INSERT INTO reco(parameter, rec, why, priority, speedup) VALUES %s", rows[1:])
        pg.conn.commit()

    sql = """
    WITH diff AS (
      SELECT s.name,
             s.setting::text                        AS cur,
             r.rec                                  AS rec,
             r.why                                  AS why,
             r.priority                             AS priority,
             r.speedup                               AS speedup,
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
    SELECT name, cur, rec,
           CASE WHEN differs THEN scope ELSE 'ok' END AS action,
           why,
           priority,
           speedup
    FROM diff
    ORDER BY differs DESC, priority DESC, name;
    """
    rows_out = pg.qall(sql)

    tbl: List[Tuple[str, str, str, str, str, str, str]] = []
    for name, cur, rec, action, why, priority, speedup in rows_out:
        tbl.append((str(name).ljust(32), str(cur).ljust(12), str(rec).ljust(12), action, why, priority, speedup))
    print_kv_table(tbl)

    # Write recommended ALTER SYSTEM statements to file if requested
    if out_file:
        stmts: List[str] = []
        # Only include settings that differ from current values
        for name, cur, rec, action, _why, _priority, _speedup in rows_out:
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
        ("3) Large tables for partitioning (>20GB)",
        """
        SELECT 
            schemaname || '.' || relname AS table_name,
            pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
            pg_size_pretty(pg_relation_size(relid)) AS table_size,
            pg_size_pretty(
                (pg_total_relation_size(relid)::bigint - pg_relation_size(relid)::bigint)
            ) AS index_size,
            n_live_tup AS live_rows,
            n_dead_tup AS dead_rows,
            round(n_dead_tup*100.0/NULLIF(n_live_tup+n_dead_tup,0),1) AS dead_pct,
            seq_scan,
            idx_scan,
            round(idx_scan*100.0/NULLIF(seq_scan+idx_scan,0),1) AS idx_scan_pct
        FROM pg_stat_user_tables
        WHERE pg_total_relation_size(relid) > 20 * 1024 * 1024 * 1024
        ORDER BY pg_total_relation_size(relid) DESC
        LIMIT 20;
        """),
        ("4) HOT updates (low %)",
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
        ("5) Seq vs Index scan",
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
        ("6) Duplicate indexes",
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
        ("7) FK without indexes",
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
        ("8) Big tables without PK",
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
        ("9) Unused indexes",
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
        ("10) Dead-tuples / bloat",
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
        ("11) Temp-files usage",
         """
         SELECT datname,
                temp_files,
                pg_size_pretty(temp_bytes) AS temp_bytes
         FROM pg_stat_database
         WHERE temp_files > 0
         ORDER BY temp_bytes DESC
         LIMIT 15;
         """),
        ("12) XID freeze age (databases)",
         """
         WITH cur AS (SELECT txid_current()::bigint AS nowxid)
         SELECT datname,
                age(datfrozenxid)                          AS age_xid,
                2000000000 - age(datfrozenxid)             AS xids_left
         FROM pg_database, cur
         ORDER BY age_xid DESC;
         """),
        ("13) XID freeze age (tables)",
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
        ("14) Wait events snapshot (0.5s)", "SNAP_WAIT"),
        ("15) Replication lag (slots)",
         """
         SELECT slot_name,
                wal_status,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained,
                active
         FROM pg_replication_slots;
         """),
        ("16) Extensions",
         """
         SELECT e.extname,
                e.extversion,
                n.nspname AS schema
         FROM pg_extension e
         JOIN pg_namespace n ON n.oid = e.extnamespace
         ORDER BY e.extname;
         """),
        ("17) HugePages / Shared memory",
         """
         SELECT name, setting FROM pg_settings
         WHERE name IN ('huge_pages','huge_page_size','shared_memory_type');
         """),
        ("18) Archiving / pg_wal size",
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
