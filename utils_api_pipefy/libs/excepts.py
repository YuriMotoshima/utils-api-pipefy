import json
import sys
from traceback import (StackSummary, TracebackException, walk_tb)
import logging

class exceptions(Exception):
    
    def __init__(self, *args: object) -> None:
        self.args = args
        self.stack_summary = StackSummary()
        self.exc_info = sys.exc_info()
        self.traceback_exception = TracebackException(*self.exc_info)
        self.str_error = self.traceback_exception._str
        self.object_error = walk_tb(self.exc_info[2])
        self.frame_summary = self.stack_summary.extract(self.object_error)
        
        self.traceback_cause()

    def traceback_cause(self):
        try:
            if self.traceback_exception.stack:
                logging.error(json.dumps({"error":self.str_error, "path":self.frame_summary[0].filename, "line":self.frame_summary[0].lineno, "code":self.frame_summary[0]._line},indent=2))
            logging.error(self.args)
        except Exception as err:
            print(err)
