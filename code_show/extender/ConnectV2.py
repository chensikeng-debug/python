from abc import ABC, abstractmethod
from typing import Dict


class ConnectV2(ABC):
    def __init__(self, data_source: str, db_name: str, conn_options: Dict[str, str] = None):
        self.data_source = data_source
        self.db_name = db_name
        self.conn_options = conn_options if conn_options else {}

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute_sql(self, sql: str):
        pass

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass
