import psycopg2
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

@dataclass
class Pg:
    dsn: str
    conn: Any = None

    def __enter__(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        return self
    
    def __exit__(self, exc_type, exc, tb):
        try:
            if self.conn:
                self.conn.close()
        finally:
            self.conn = None
    
    def begin(self):
        self.conn.autocommit = False
    
    def rollback(self):
        self.conn.rollback()
    
    def commit(self):
        self.conn.commit()

    def qval(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Any]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None

    def qrow(self, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Tuple[Any, ...]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def qall(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Tuple[Any, ...]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)    