import os
import sys
import unittest
from glob import glob
from daemon import runner
from inspect import isclass
from importlib import import_module

from daemonizer.storage.sql import MySQL

class DaemonExecutioner:

    DEFAULT_PROCESS_STRING = 'daemons.%s'

    def __init__(self, daemon_name, pname, action, sleep_time=None, pid_timeout=None, is_testing=False):
        """
        Create daemon and process attributes for the given object
        """
        self.pname = pname
        self.action = action
        self.daemon_name = self.DEFAULT_PROCESS_STRING % (daemon_name, )

        self.daemon = import_module(self.daemon_name)
        
        self.kwargs = {}
        self.kwargs['action_type'] = action
        if sleep_time:
            self.kwargs['sleep_time'] = sleep_time
        if pid_timeout:
            self.kwargs['pid_timeout'] = pid_timeout
        if is_testing:
            self.kwargs['is_testing'] = is_testing

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

class DaemonTester:

    DEFAULT_DAEMON_TEST_STRING = 'daemons.%s.tests'
    
    def __init__(self, daemon, test_name):
        self.test_name = test_name
        self.daemon_name = self.DEFAULT_DAEMON_TEST_STRING % (daemon, )

        self.daemon = import_module(self.daemon_name)

    def _get_all_test_modules(self):
        """
        Steps:
        1. Find the base path of given package
        2. Access all the modules belonging to the specified package
        """
        l = []
        module_path = os.path.abspath(self.daemon.__file__).replace('__init__.pyc', 'test_*.py')
        tests_path_list = glob(module_path)
        
        for path in tests_path_list:
            test_module_name = path.split('/')[-1].replace('.py', '')
            test_module = import_module('.'.join([self.daemon_name, test_module_name]))
            l.append(test_module)

        return l

    def _get_all_tests(self):
        test_modules_list = self._get_all_test_modules()        

        d = {}
        for test_module in test_modules_list:
            for key, val in test_module.__dict__.items():
                # Extraction of valid test cases
                if isinstance(val, type) and isclass(val) and issubclass(val, unittest.case.TestCase):
                    d[key] = val

        return d

    def execute(self):
        suite_list = []

        all_tests = self._get_all_tests()

        # Validation check for the test names provided
        if self.test_name and self.test_name not in all_tests:
            raise Exception("Invalid Tests Found: '%s' (Execution Halted)" % (self.test_name, ))
        elif self.test_name:
            modules_to_run = [self.test_name]
        else:
            # Import all tests for the given daemon app
            modules_to_run = self._get_all_tests()

        # Run tests
        for module_name in modules_to_run:
            module = all_tests.get(module_name)
            loader = unittest.TestLoader().loadTestsFromTestCase(module)

            suite_list.append(loader)

        suite_all = unittest.TestSuite(suite_list)
        unittest.TextTestRunner(verbosity=2).run(suite_all)

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

def execute_process_from_cmd(action, daemon, pname, sleep_time, pid_timeout, is_testing):
    """
    Executes the corresponding tasks for a given daemon.
    """

    if action in ['start', 'stop', 'restart']:
        execution_master = DaemonExecutioner(daemon, pname, action, sleep_time, pid_timeout, is_testing)
    elif action == 'test':
        execution_master = DaemonTester(daemon, pname)
    elif action == 'register':
        execution_master = ProcessRegisterar(daemon, pname)
    else:
        return

    execution_master.execute()