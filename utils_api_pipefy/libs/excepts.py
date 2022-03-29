from os import getcwd
import sys
import traceback
import logging
from dotenv import load_dotenv
from utils_api_pipefy.libs.log import log

load_dotenv(dotenv_path=f"{getcwd()}/.env")
log().loginit()


class exceptions(Exception):
    
    def __init__(self, *args: object) -> None:
        self.args = args
        self.exc_info = sys.exc_info()
        self.traceback_cause()


    def traceback_cause(self):
        exc = traceback.TracebackException(*self.exc_info)
        expected_stack = traceback.StackSummary.extract(traceback.walk_tb(self.exc_info[2]))
        logging.error(f"1 - {exc}\n2 - {expected_stack[0]}\n")