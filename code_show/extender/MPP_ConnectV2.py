from typing import Dict
import psycopg2.pool

from code_show.extender.ConnectV2 import ConnectV2


class MPP_ConnectV2(ConnectV2):
    def __init__(self, data_source: str, db_name: str, conn_options: Dict[str, str] = None,
                 minconn: int = 1, maxconn: int = 10):
        super().__init__(data_source, db_name, conn_options)
        self.minconn = minconn
        self.maxconn = maxconn
        self.pool = None

    def connect(self):
        if not self.pool:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn, self.maxconn,
                host=self.conn_options.get('host', ''),
                port=self.conn_options.get('port', ''),
                user=self.conn_options.get('user', ''),
                password=self.conn_options.get('password', ''),
                database=self.db_name,
            )
        return self.pool.getconn()

    def execute_sql(self, sql: str):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()

    def begin_transaction(self):
        conn = self.connect()
        conn.autocommit = False
        return conn

    def commit(self, conn=None):
        if conn is None:
            return
        try:
            conn.commit()
        finally:
            self.release(conn)

    def rollback(self, conn=None):
        if conn is None:
            return
        try:
            conn.rollback()
        finally:
            self.release(conn)

    def release(self, conn):
        self.pool.putconn(conn)
