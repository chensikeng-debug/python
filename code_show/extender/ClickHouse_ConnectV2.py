from code_show.extender.ConnectV2 import ConnectV2
import clickhouse_driver
from typing import Dict


class ClickHouse_ConnectV2(ConnectV2):
    def __init__(self, data_source: str, db_name: str, conn_options: Dict[str, str] = None):
        super().__init__(data_source, db_name, conn_options)

    def connect(self):
        return clickhouse_driver.connect(
            host=self.conn_options.get('host', ''),
            port=int(self.conn_options.get('port', '9000')),
            user=self.conn_options.get('user', ''),
            password=self.conn_options.get('password', ''),
            database=self.db_name,
        )

    def execute_sql(self, sql: str):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()

    def begin_transaction(self):
        conn = self.connect()
        conn.set_autocommit(False)
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
        conn.close()
