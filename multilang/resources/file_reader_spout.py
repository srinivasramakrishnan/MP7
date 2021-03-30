# import os
# from os.path import join
from time import sleep

# from streamparse import Spout
import storm


class FileReaderSpout(storm.Spout):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._complete = False

        storm.logInfo("Spout instance starting...")

        # TODO:
        # Task: Initialize the file reader
        self._myreadfilepointer = open("/tmp/data.txt")
        pass
        # End

    def nextTuple(self):
        # TODO:
        # Task 1: read the next line and emit a tuple for it
        line = self._myreadfilepointer.readline()

        # Task 2: don't forget to sleep for 1 second when the file is entirely read to prevent a busy-loop
        if line:
            storm.logInfo("%s" %line)
            storm.emit([line])
        else:
            sleep(1)

        pass
        # End


# Start the spout when it's invoked
FileReaderSpout().run()