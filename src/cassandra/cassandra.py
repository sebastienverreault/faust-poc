import logging
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

from src.WebLogs.ReducedLog import ReducedLog


class CassandraDriver:

    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(['67.205.190.73'])
        self.session = self.cluster.connect(self.keyspace)

    def getsession(self):
        return self.session

    def setlogger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            self.log.info("dropping existing keyspace...")
            self.session.execute("DROP KEYSPACE " + keyspace)

        self.log.info("creating keyspace...")
        self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                """ % keyspace)

        self.log.info("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_table(self):
        c_sql = """
                    CREATE TABLE IF NOT EXISTS weblogs.logs (
                        ip_address text,
                        user_agent text,
                        request text,
                        byte_ranges list<text>,
                        PRIMARY KEY (ip_address, user_agent, request)                    );
                 """
        self.session.execute(c_sql)
        self.log.info("Table Created !!!")

    # lets do some batch insert
    def insert_data(self, data: ReducedLog):
        csql = f"INSERT INTO logs (ip_address, user_agent , request, byte_ranges) " \
               f"VALUES ({data.IpAddress}, {data.UserAgent}, {data.Request}, [{data.LoByte,}, {data.HiByte}]) IF NOT EXISTS"
        # insert_sql = self.session.prepare(csql)
        # stmt = BatchStatement()
        # stmt.add(insert_sql, ('data'))
        stmt = SimpleStatement(csql)
        self.session.execute(stmt, data)
        self.log.info('Batch Insert Completed')

    # def select_data(self):
    #     rows = self.session.execute('select * from logs limit 5;')
    #     for row in rows:
    #         print(row.ename, row.sal)
    #
    # def update_data(self):
    #     pass
    #
    # def delete_data(self):
    #     pass
