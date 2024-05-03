import json
import logging
import sys
from traceback import StackSummary, TracebackException, walk_tb


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
                logging.error(json.dumps({frame[0]: {"error": self.str_error, "path": frame[1].filename,
                              "line": frame[1].lineno, "code": frame[1]._line} for frame in enumerate(self.frame_summary)}, indent=2))
            logging.error(self.args)
        except Exception as err:
            print(err)
