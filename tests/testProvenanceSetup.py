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
from lsst.ctrl.provenance import ProvenanceSetup

logger = Log(Log.getDefaultLog(), "tester")
logger.setThreshold(Log.FATAL+10)

class GoodRecorder(ProvenanceRecorder):
    def __init__(self, testcase, logger=None):
        ProvenanceRecorder.__init__(self, logger, True)
        self.tester = testcase

    def record(self, filename):
        self.tester.assert_(len(filename) > 0, "empty filename found")
        self._logger.log(Log.INFO, "recording "+filename)
        self.tester.recorded += 1

class ProvenanceSetupTestCase(unittest.TestCase):

    def setUp(self):
        self.setup = ProvenanceSetup()
        self.rec = GoodRecorder(self, logger)
        self.recorded = 0

    def tearDown(self):
        pass

    def testProdRecorder(self):
        recs = self.setup.getRecorders()
        self.assertEquals(len(recs), 0)
        self.setup.addProductionRecorder(self.rec)
        recs = self.setup.getRecorders()
        self.assertEquals(len(recs), 1)

    def testAddFile(self):
        files = self.setup.getFiles()
        self.assertEquals(len(files), 0)

        self.setup.addProductionPolicy("goober.paf")
        files = self.setup.getFiles()
        self.assertEquals(len(files), 1)
        self.assertEquals(files[0], "goober.paf")

        files[0] = "hank.paf"
        files = self.setup.getFiles()
        self.assertEquals(len(files), 1)
        self.assertEquals(files[0], "goober.paf")

        self.setup.addProductionPolicy("hank.paf")
        files = self.setup.getFiles()
        self.assertEquals(len(files), 2)
        self.assertEquals(files[0], "goober.paf")
        self.assertEquals(files[1], "hank.paf")

        

    def testRecord(self):
        self.setup.addProductionPolicy("hank.paf")
        self.setup.addProductionRecorder(self.rec)
        self.assertEquals(len(self.setup.getRecorders()), 1)

        self.assertEquals(self.recorded, 0)
        self.setup.recordProduction()
        self.assertEquals(self.recorded, 1)

        self.setup.addProductionRecorder(self.rec)
        self.assertEquals(len(self.setup.getRecorders()), 2)
        self.setup.recordProduction()
        self.assertEquals(self.recorded, 3)

    def testAddCmd(self):
        self.assertEquals(len(self.setup.getCmds()), 0)
        self.assertEquals(len(self.setup.getCmdPaths()), 0)

        self.setup.addWorkflowRecordCmd("recProv.py", "-v 4".split())
        cmds = self.setup.getCmds()
        paths = self.setup.getCmdPaths()
        self.assertEquals(len(cmds), 1)
        self.assertEquals(len(paths), 1)
        self.assert_(isinstance(cmds[0], list), "command list not a tuple")
        self.assert_(paths[0] is None, "found path when one not given")
        self.assertEquals(cmds[0][0], "recProv.py")
        self.assertEquals(cmds[0][1], "-v")
        self.assertEquals(cmds[0][2], "4")
        
        self.setup.addWorkflowRecordCmd("dbingest.py", "-u ray".split(),
                                        "/usr/local/bin/dbingest.py")
        cmds = self.setup.getCmds()
        paths = self.setup.getCmdPaths()
        self.assertEquals(len(cmds), 2)
        self.assertEquals(len(paths), 2)
        self.assertEquals(cmds[0][0], "recProv.py")
        self.assertEquals(cmds[0][1], "-v")
        self.assertEquals(cmds[0][2], "4")
        self.assertEquals(cmds[1][0], "dbingest.py")
        self.assertEquals(cmds[1][1], "-u")
        self.assertEquals(cmds[1][2], "ray")
        self.assertEquals(paths[1], "/usr/local/bin/dbingest.py")
        
        

    
__all__ = "ProvenanceSetupTestCase".split()        

if __name__ == "__main__":
    unittest.main()

