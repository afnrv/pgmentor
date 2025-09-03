from pgmentor.db import Pg
import json

def analyze_query(pg: Pg, query: str) -> str:
    sql = """ EXPLAIN (COSTS TRUE) %s """ % query
    plan_json = pg.qval(sql)

    total_time = plan_json[0]["Plan"]["Total Cost"] * 0,01
    total_volume = plan_json[0]["Plan"]["Plan Rows"] * plan_json[0]["Plan"]["Plan Width"]

    return f"""Total time: {total_time} seconds, 
               Total volume: {total_volume} bytes"""