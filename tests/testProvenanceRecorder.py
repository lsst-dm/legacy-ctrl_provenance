#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

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

    
