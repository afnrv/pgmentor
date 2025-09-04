from pgmentor.db import Pg
from openai import OpenAI
import os

def calibrate_cost_to_time(pg) -> float:
    query = "SELECT COUNT(*) FROM pg_class"

    plan = pg.qval(f"EXPLAIN (FORMAT JSON) {query}")
    total_cost = plan[0]["Plan"]["Total Cost"]

    analyze_plan = pg.qval(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
    actual_time = analyze_plan[0]["Plan"]["Actual Total Time"] / 1000.0

    if total_cost > 0:
        return actual_time / total_cost
    return 0.001


def analyze_query(pg, query: str) -> str:
    sql = f"EXPLAIN (COSTS TRUE, FORMAT JSON, VERBOSE) {query}"
    result = pg.qval(sql)
    plan_json = result[0]
    
    plan = plan_json["Plan"]
    total_cost = plan["Total Cost"]
    est_rows = plan["Plan Rows"]
    est_width = plan["Plan Width"]
    total_volume = est_rows * est_width

    seq_page_cost = float(pg.qval("SHOW seq_page_cost")[0][0])
    random_page_cost = float(pg.qval("SHOW random_page_cost")[0][0])
    work_mem = pg.qval("SHOW work_mem")[0][0]

    cost_to_sec = calibrate_cost_to_time(pg)
    total_time = total_cost * cost_to_sec\

    analysis = "\n" + "="*60 + "\n" +  ' '*22 +"Query Statistics" + "\n" + "="*60 + "\n"
    analysis += f"Total cost: {total_cost}\n"
    analysis += f"Estimated time: {total_time:.6f} s\n"
    analysis += f"Estimated rows: {est_rows}\n"
    analysis += f"Estimated row size: {est_width} bytes\n"
    analysis += f"Estimated volume: {total_volume} bytes\n"
    analysis += f"work_mem: {work_mem}\n"
    analysis += f"seq_page_cost: {seq_page_cost}, random_page_cost: {random_page_cost}\n"

    if "Relation Name" in plan:
        relname = plan["Relation Name"]
        relinfo = pg.qrow("""
            SELECT relpages, reltuples
            FROM pg_class
            WHERE relname = %s
        """, (relname,))
        if relinfo:
            relpages, reltuples = relinfo
            analysis += f"\nRelation {relname}: {relpages} pages, {reltuples} tuples\n"

    locks = analyze_locks(pg, query)
    if locks:
        analysis += "\n" + "="*60 + "\n" + ' '*27 + "Locks" + "\n" + "="*60 + "\n" + locks

    optimize = optimize_query(query)

    analysis += f"\n" + "="*60 + "\n" + ' '*27 + "Optimization" + "\n" + "="*60 + "\n" + optimize + "\n" + "="*60 + "\n"

    return analysis

def analyze_locks(pg: Pg, query: str) -> str:
    pg.begin()
    pid = pg.qval("SELECT pg_backend_pid();")

    try:
        pg.exec(query);
    except Exception as e:
        pg.rollback()
        return None

    locks = pg.qall("""
        SELECT 
            l.locktype,
            l.mode,
            l.granted,
            COALESCE(c.relname, 'N/A') AS relation_name,
            l.page,
            l.tuple,
            l.virtualxid,
            l.transactionid
        FROM pg_locks l
        LEFT JOIN pg_class c ON c.oid = l.relation
        WHERE l.pid = %s
        ORDER BY l.granted DESC, l.mode;
    """, (pid,))

    res_locks = f"Locks held by PID {pid}:\n"
    if not locks:
        res_locks += " No locks held."
    else:
        for lock in locks:
            locktype, mode, granted, relname, page, tup, vxid, xid = lock
            status = "GRANTED" if granted else "WAITING"
            obj = "unknown"
            if relname != 'N/A':
                obj = f"table='{relname}'"
                if page is not None and tup is not None:
                    obj += f" (page={page}, tuple={tup})"
            elif xid:
                obj = f"transaction {xid}"
            elif vxid:
                obj = f"virtual xid {vxid}"
            else:
                obj = locktype

            res_locks += f" [{status}] {mode} on {locktype} → {obj}\n"


    blockers = pg.qall("""
        SELECT 
            blocked.pid AS blocked_pid,
            substring(blocked.query for 60) AS blocked_query,
            now() - blocked.query_start AS duration
        FROM pg_locks l
        JOIN pg_stat_activity blocked ON blocked.pid = l.pid
        WHERE l.pid != %s
          AND l.locktype IN ('relation', 'tuple', 'transactionid')
          AND l.transactionid IN (
            SELECT transactionid FROM pg_locks WHERE pid = %s AND granted
          )
          AND NOT l.granted;
    """, (pid, pid))

    res_locks += f"\n Who is blocked by PID {pid}?\n"
    if not blockers:
        res_locks += "  No one is blocked (at the moment).\n"
    else:
        for blocked_pid, bquery, duration in blockers:
            res_locks += f"  PID {blocked_pid} is waiting: '{bquery.strip()}...' | ⏱️ {duration}\n"
    
    pg.rollback()
    pg.conn.autocommit = True
    return res_locks
    

def optimize_query(query: str) -> str:
    client_deepseek = OpenAI(api_key=os.environ.get("API_KEY_DEEPSEEK"), base_url="https://api.deepseek.com", timeout=180.0)

    INIT_CONTENT = """
    You are a sql query optimizer. 
    Suggest only one best option, then justify point by point why it is better. 
    Then suggest indexes that must exist for this query so that it works as efficiently as possible. Don't use Markdown.
    """
    try:
        response = client_deepseek.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": INIT_CONTENT},
                {"role": "user", "content": query},
            ],
            stream=False
            )
        
        reply = response.choices[0].message.content.strip()
    
    except Exception as e:
        return f"Error accessing AI: {e}."  
    
    return reply