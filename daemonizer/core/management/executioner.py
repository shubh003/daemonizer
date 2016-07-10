import os
import sys
import shutil
import unittest
from glob import glob
from daemon import runner
from inspect import isclass
from importlib import import_module

from daemonizer.storage.sql import MySQL

class DaemonExecutioner:

    DEFAULT_PROCESS_STRING = 'daemons.%s'

    def __init__(self, daemon_name, pname, action, sleep_time=None, pid_timeout=None):
        """
        Create daemon and process attributes for the given object
        """
        self.pname = pname
        self.action = action
        self.daemon_name = self.DEFAULT_PROCESS_STRING % (daemon_name, )

        self.daemon = import_module(self.daemon_name)
        self.action = action
        
        self.kwargs = {}
        self.kwargs['action_type'] = action
        if sleep_time:
            self.kwargs['sleep_time'] = sleep_time
        if pid_timeout:
            self.kwargs['pid_timeout'] = pid_timeout

    def _success_error_display(self, error_log):
        """
        Display error message per process in following format:
        
        <Process> failed due to following error:
        <Exception>
        """
        if not error_log:
            return "All Actions Done Successfully!"

        msg = ''
        pattern_print = """'%s' process failed due to following error:\n%s\n"""
        for process, ex in error_log.items():
            msg += pattern_print % (process, ex)

        return msg

    def execute(self):

        error_log = {}
        try:
            process_to_run = getattr(self.daemon, self.pname)(**self.kwargs)
            
            # Run the process as daemon
            daemon_runner = runner.DaemonRunner(process_to_run)
            daemon_runner.do_action()

            # Stop action registering done here
            # Because after stopping, we dont have sql connection instance
            if self.action == 'stop':
                process_name = process_to_run.__class__.__name__
                sql = MySQL()

                sql.append_action(process_name, 'stop')
                sql.close()


        except Exception as ex:
            error_log[self.pname] = ex

        print self._success_error_display(error_log)

class ProcessRegisterar:
    
    DEFAULT_DAEMON_STRING = 'daemons.%s'

    def __init__(self, daemon_name, plist):
        self.sql_conn = MySQL()

        if daemon_name:
            self.daemon_name = self.DEFAULT_DAEMON_STRING % daemon_name
        else:
            self.daemon_name = daemon_name
        self.plist = [plist]

    def _get_all_processes(self, daemon):
        l = []
        for key, val in daemon.__dict__.items():
            if isclass(val):
                l.append(key)

        return l

    def execute(self):
        """
        Register new tasks
        """
        # Append daemon_name to default string type
        # or in case daemon_name is not there, get all daemons
        if not self.daemon_name:
            daemon_list = settings.INSTALLED_APPS
        else:
            daemon_list = [self.daemon_name]

        # extract all processes for give daemon list
        # if prcesses are not provided
        if not self.plist:
            self.plist = []
            for daemon_name in daemon_list:
                daemon = import_module(daemon_name)
                self.plist += self._get_all_processes(daemon)

        existing_registered = []
        newly_registered = []

        for pname in self.plist:
            if pname in self.sql_conn.pdict:
                existing_registered.append(pname)
                continue
            
            newly_registered.append(pname)
            
        self.sql_conn.register_processes(newly_registered)
        self.sql_conn.close()

        print "<Success>\nFollowing processes added to task list:\n%s" % str(newly_registered)
        if existing_registered:
            print "Already Registered Processes found:\n%s" % str(existing_registered)

def execute_process_from_cmd(action, daemon, pname, sleep_time, pid_timeout):
    """
    Executes the corresponding tasks for a given daemon.
    """

    if action in ['start', 'stop', 'restart']:
        execution_master = DaemonExecutioner(daemon, pname, action, sleep_time, pid_timeout)
    elif action == 'test':
        print "Not implemented yet."
        """
        To be done
        """
        #execution_master = TestsExecutioner(runner_name, runner_list)
    elif action == 'register':
        execution_master = ProcessRegisterar(daemon, pname)
    else:
        return

    execution_master.execute()