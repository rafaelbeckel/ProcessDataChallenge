import tblib.pickling_support
tblib.pickling_support.install()

import sys

class DelayedException(Exception):

    def __init__(self, exception):
        self.exception = exception
        __,  __, self.traceback = sys.exc_info()
        super(DelayedException, self).__init__(str(exception))

    def re_raise(self):
        raise self.exception.with_traceback(self.traceback)