import psycopg2
from typing import Any, Dict, List


class PostgresSql:

    conn = None
    cur = None
    conf = None

    def __init__(self, **kwargs) -> None:
        self.conf = kwargs
        self.conf['host'] = kwargs.get("host", "127.0.0.1")
        self.conf['port'] = kwargs.get("port", 5432)
        self.conf['db'] = kwargs.get("sb")
        self.conf['user'] = kwargs.get("user")
        self.conf['password'] = kwargs.get("password")
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(host=self.conf.get('host'),
                                         database=self.conf.get('db'),
                                         user=self.conf.get('user'),
                                         password=self.conf.get('password'),
                                         port=self.conf.get('port'))
            self.cur = self.conn.cursor()
            self.conn.autocommit = self.conf.get('autocommit')

        except Exception as e:
            print("Postgres connection failed")
            raise ValueError("Connection to the database failed.")

    def insert(self,
               schema: str,
               table: str,
               data) -> int:
        query = self.__serialize_insert(data)
        sql = "INSERT INTO %s.%s (%s) VALUES(%s)" % (
            schema, table, query[0], query[1])
        return self.query(sql, tuple(data.values())).rowcount

    def __serialize_insert(self, data: Dict[str, Any]) -> List[str]:
        keys = ",".join(data.keys())
        print(keys)
        vals = ",".join(["%s" for k in data])
        print(type(vals))
        return [keys, vals]

    def query(self, query, params=None):
        # Write a connetion decorator here. If connection is not alive then try for reconnect. Max retries are 3.
        # This method should also handle all the execeptions reaised by the database.
        self.cur.execute(query, params)
        return self.cur
