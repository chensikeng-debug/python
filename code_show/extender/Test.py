# 创建TiDB_ConnectV2对象并连接到数据库
from code_show.extender.ClickHouse_ConnectV2 import ClickHouse_ConnectV2
from code_show.extender.TiDB_ConnectV2 import TiDB_ConnectV2

tidb_conn = TiDB_ConnectV2(data_source='mysql', db_name='mydb', conn_options={'host': 'localhost', 'user': 'myuser', 'password': 'mypassword'})
results = tidb_conn.execute_sql('SELECT * FROM mytable')  # 执行SQL语句并获取查询结果

# 创建ClickHouse_ConnectV2对象并连接到数据库
ch_conn = ClickHouse_ConnectV2(data_source='clickhouse', db_name='mydb', conn_options={'host': 'localhost', 'user': 'myuser', 'password': 'mypassword'})
results = ch_conn.execute_sql('SELECT * FROM mytable')  # 执行SQL语句并获取查询结果