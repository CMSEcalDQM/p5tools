import os
import time

class Logger(object):
    def __init__(self, path_):
        self._file = open(path_, 'a')

    def __del__(self):
        self._file.close()

    def write(self, *args_):
        self._file.write('[' + time.asctime() + ']')
        for arg in args_:
            self._file.write(' ' + str(arg).strip())
        self._file.write('\n')
        self._file.flush()
        os.fsync(self._file)

    def close(self):
        self._file.close()
