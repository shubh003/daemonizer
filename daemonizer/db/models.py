import abc
import time
from daemon import runner

from daemonizer.storage.sql import MySQL

class Daemonizer:

    __metaclass__ = abc.ABCMeta

    def __init__(self, action_type, sleep_time=10, pid_timeout=5):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/%s.pid' % (self.__class__.__name__, )
        
        self.action_type = action_type
        
        self.pidfile_timeout = pid_timeout
        self.sleep_time = sleep_time

    def run(self):
        self.connect_to_storage()
        self.register_process_action()
        
        while True:
            self.start_process()
            
            time.sleep(self.sleep_time)

    def connect_to_storage(self):
        """
        Connect to given storage
        """
        self.sql = MySQL()

    def register_process_action(self):
        """
        Register any action taken on the process
        """
        self.sql.append_action(self.__class__.__name__, self.action_type)

    def register_task_completion(self, task_type, description):
        """
        Register task completion to server
        """
        self.sql.append_task_completion(self.__class__.__name__, task_type, description)

    @abc.abstractmethod
    def start_process(self):
        """
        Start the given process
        """