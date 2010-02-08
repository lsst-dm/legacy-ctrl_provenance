#!/usr/bin/env python
"""
Tests of the ProvenanceRecorder
"""
import pdb                              # we may want to say pdb.set_trace()
import os
import sys
import unittest
import time

from lsst.pex.logging import Log
from lsst.ctrl.provenance import ProvenanceRecorder

class ProvenanceRecorderTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testBadInit(self):
        self.assertRaises(RuntimeError, ProvenanceRecorder)

    def testBadSubclass(self):
        logger = Log(Log.getDefaultLog(), "tester")
        logger.setThreshold(Log.FATAL+10)
        recorder = BadRecorder(logger)
        self.assertRaises(RuntimeError, recorder.record, "file")

    def testGoodSubclass(self):
        logger = Log(Log.getDefaultLog(), "tester")
        logger.setThreshold(Log.FATAL+10)
        recorder = GoodRecorder(self, logger)
        recorder.record("goober.paf")

class BadRecorder(ProvenanceRecorder):
    def __init__(self, logger=None):
        ProvenanceRecorder.__init__(self, logger, True)

class GoodRecorder(ProvenanceRecorder):
    def __init__(self, testcase, logger=None):
        ProvenanceRecorder.__init__(self, logger, True)
        self.tester = testcase

    def record(self, filename):
        self.tester.assert_(len(filename) > 0, "empty filename found")
        self._logger.log(Log.INFO, "recording "+filename)

__all__ = "ProvenanceRecorderTestCase".split()        

if __name__ == "__main__":
    unittest.main()

    
