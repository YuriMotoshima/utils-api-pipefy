import json
import logging
import sys
from traceback import (StackSummary, TracebackException, format_exception,
                       walk_tb)

class exceptions(Exception):
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.error_type = type(error).__name__
        self.error_message = str(error)
        
        self.formatted_frames = None
        self.exc_info = sys.exc_info()
        self.traceback_exception = TracebackException(*self.exc_info)
        self.str_error = ''.join(format_exception(*self.exc_info))
        self.tb = self.exc_info[2]
        self.stack_summary = StackSummary.extract(walk_tb(self.tb))
        
        self.traceback_cause()
        super().__init__(self.error_message)
        
    def traceback_cause(self):
        try:
            if self.stack_summary:
                self.formatted_frames = {
                    f"{i}": {
                        "error_type": self.error_type,
                        "error": self.error_message,
                        "path": frame.filename,
                        "line": frame.lineno,
                        "code": frame.line
                    }
                    for i, frame in enumerate(self.stack_summary)
                }
                logging.error(json.dumps(self.formatted_frames, indent=2))
            else:
                logging.error("Sem stack summary para exibir.")
                
            logging.error(f"{self.error_type}: {self.error_message}")
            
        except Exception as err:
            logging.warning(f"Erro ao processar traceback_cause: {err}")