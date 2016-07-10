import MySQLdb
from daemonizer.conf import settings

SQL_HOST = settings.SQL_HOST
SQL_USER = settings.SQL_USER
SQL_PASSWORD = settings.SQL_PASSWORD
SQL_DB = settings.SQL_DB

class MySQL:

    Q_PROCESS_IDS = """SELECT id, pname FROM process_list"""
    Q_REGISTER = """INSERT INTO process_list (pname) VALUES ('%s')"""
    Q_ACTION = """INSERT INTO process_actions (pid, action) VALUES (%s, '%s')"""
    Q_TASK = """INSERT INTO process_tasks (pid, task_type, description) VALUES (%s, '%s', '%s')"""

    def __init__(self):
        self.conn = None
        self.connect()
        
        self.pdict = {}
        self._get_process_ids()

    def connect(self, host=SQL_HOST, user=SQL_USER, password=SQL_PASSWORD, db=SQL_DB):
        self.conn = MySQLdb.connect(host, user, password, db)
        self.cursor = self.conn.cursor()
        self.conn.autocommit(True)

    def close(self):
        if self.conn:
            self.conn.close()

    def _get_process_ids(self):
        self.execute_query(self.Q_PROCESS_IDS)

        fetched = self.cursor.fetchall()
        if fetched:
            for pid, pname in fetched:
                self.pdict[pname] = pid

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            self.cursor.execute(query)

    def register_processes(self, pname_list):
        """
        Register a process here
        """
        for pname in pname_list:
            q_reg = self.Q_REGISTER % (pname, )
            self.execute_query(q_reg)

    def append_action(self, pname, action):
        """
        Append actions done on a given process
        """
        if pname not in self.pdict:
            raise Exception("Process '%s' does not exist in database!" % (pname, ))

        pid = self.pdict.get(pname)

        q_act = self.Q_ACTION % (pid, action)
        self.execute_query(q_act)

    def append_task_completion(self, pname, task_type, description):
        """
        Append tasks completed by a given process
        """
        if pname not in self.pdict:
            raise Exception("Process '%s' does not exist in database!" % (pname, ))

        pid = self.pdict.get(pname)

        q_task = self.Q_TASK % (pid, task_type, description)
        self.execute_query(q_task)