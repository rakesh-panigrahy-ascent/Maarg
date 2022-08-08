from abc import ABC, abstractmethod
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
import pymssql


# from resources.portcheck import checkHost
class HostNotReachable(Exception):
    pass


class Database(ABC):
    def __init__(self, user, password, database, host, port, schema=None, secondary_host=None):
        self.host = host
        self.secondary_host = secondary_host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        self.conn = None

    @abstractmethod
    def connect(self):
        if checkHost(self.host, self.port):
            self.active_host = self.host
        elif self.secondary_host is not None and checkHost(self.secondary_host, self.port):
            print("Primary host not reachable. Trying secondary...")
            self.active_host = self.secondary_host
        else:
            raise HostNotReachable("Both Primary and Secondary IP not reachable.")

    def query2df(self, query, parse_dates=None):
        if self.conn is None:
            self.connect()
        try:
            print('Connected. Executing Query.')
            df = pd.read_sql(query, self.conn, parse_dates=parse_dates)
            return df
        except Exception as e:
            print(e)
        finally:
            if self.conn is not None:
                self.close()

    def table2df(self, table_name, cols=None):
        if cols is None:
            colstr = '*'
        else:
            colstr = ','.join(cols)
        query = 'select {colstr} from {tab};'.format(colstr=colstr, tab=table_name)
        return self.query2df(query)

    def execute(self, query):
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        try:
            result = cur.execute(query)
            cur.close()
            self.conn.commit()
            if hasattr(cur, 'statusmessage'):
                print("STATUS MESSAGE: {}".format(cur.statusmessage))
            else:
                print("ROWS AFFECTED: {}".format(cur.rowcount))
            return result
        finally:
            if cur is not None:
                cur.close()
            if self.conn is not None:
                self.close()

    def execute_return(self, query):
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            result = cur.fetchall()
            self.conn.commit()
            return result
        finally:
            if cur is not None:
                cur.close()
            if self.conn is not None:
                self.close()

    def getConnection(self):
        return self.conn

    def close(self):
        self.conn.close()
        self.conn = None


class PostgreSQL(Database):
    default_port = 5432

    def __init__(self, user, password, database, host, port=default_port, schema=None):
        super().__init__(user, password, database, host, port, schema)

    def connect(self):
        self.conn = psycopg2.connect(
            "host={} port={} dbname={} user={} password={} options={}".format(self.host, self.port,
                                                                              self.database, self.user,
                                                                              self.password, '\'-c search_path=' + str(
                    self.schema) + '\''))

    def create_engine_psql(self):
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}',
                               connect_args={'options': '-c search_path={}'.format(self.schema)})
        return engine

    def execute_vals(self, query, data=None, fetch=False, pagesize=100):
        if data is None:
            d = data
        elif len(data) == 0:
            return None
        elif type(data) == list:
            d = data
        else:
            d = list(zip(*[data[c].values.tolist() for c in data]))
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        try:
            resp = psycopg2.extras.execute_values(cur, query, d, fetch=fetch, page_size=pagesize)
            self.conn.commit()
            if hasattr(cur, 'statusmessage'):
                print("STATUS MESSAGE: {}".format(cur.statusmessage))
            else:
                print("ROWS AFFECTED: {}".format(cur.rowcount))
            return (resp)
        finally:
            if cur is not None:
                cur.close()
            if self.conn is not None:
                self.close()


class Redshift(PostgreSQL):
    default_port = 5439

    def __init__(self, user, password, database, host, port=default_port, schema=None):
        super().__init__(user, password, database, host, port, schema)


class MSSQL(Database):
    default_port = 1433

    def __init__(self, user, password, database, host, port=default_port, schema=None):
        super().__init__(user, password, database, host, port, schema)

    def connect(self):
        self.conn = pymssql.connect(host=self.host, port=self.port, database=self.database, user=self.user,
                                    password=self.password)
