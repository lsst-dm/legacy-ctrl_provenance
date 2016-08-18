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
from __future__ import print_function
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
repos = os.path.join(os.environ['CTRL_PROVENANCE_DIR'], "tests", "policy")
prodPolicyFile = os.path.join(repos, "production.paf")


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

        self.setup.addProductionPolicyFile("goober.paf")
        files = self.setup.getFiles()
        self.assertEquals(len(files), 1)
        self.assertEquals(files[0], "goober.paf")

        files[0] = "hank.paf"
        files = self.setup.getFiles()
        self.assertEquals(len(files), 1)
        self.assertEquals(files[0], "goober.paf")

        self.setup.addProductionPolicyFile("hank.paf")
        files = self.setup.getFiles()
        self.assertEquals(len(files), 2)
        self.assertEquals(files[0], "goober.paf")
        self.assertEquals(files[1], "hank.paf")

    def testAddAllFiles(self):
        # see FindFilesTestCase for test of underlying function
        self.assertEquals(len(self.setup.getFiles()), 0)
        self.setup.addAllProductionPolicyFiles(prodPolicyFile, repos)
        files = self.setup.getFiles()
        self.assertEquals(len(files), 4)

        find = [prodPolicyFile] + map(lambda f: os.path.join(repos, f),
                                      "lsst10-mysql.paf database/dc3a.paf platform/abecluster.paf".split())
        for file in find:
            self.assert_(file in files, "Failed to file file: "+file)

        self.setup.addProductionRecorder(self.rec)
        self.assertEquals(self.recorded, 0)
        self.setup.recordProduction()
        self.assertEquals(self.recorded, 4)

    def testRecord(self):
        self.setup.addProductionPolicyFile("hank.paf")
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
        print("=======================================")
        print(cmds)
        print(paths)
        print("=======================================")
        self.assertEquals(len(cmds), 2)
        self.assertEquals(len(paths), 2)
        self.assertEquals(cmds[0][0], "recProv.py")
        self.assertEquals(cmds[0][1], "-v")
        self.assertEquals(cmds[0][2], "4")
        self.assertEquals(cmds[1][0], "dbingest.py")
        self.assertEquals(cmds[1][1], "-u")
        self.assertEquals(cmds[1][2], "ray")
        self.assertEquals(paths[1], "/usr/local/bin/dbingest.py")


class FindFilesTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testExtractProdFiles(self):
        files = set(ProvenanceSetup.extractIncludedFilenames(prodPolicyFile,
                                                             repos,
                                                             pipefile="workflow.pipeline.definition"))

        find = "lsst10-mysql.paf database/dc3a.paf platform/abecluster.paf".split()
        self.assertEquals(len(files), len(find))

        for file in find:
            self.assert_(file in files, "Failed to file file: "+file)

    def testExtractProdFiles2(self):
        files = set(ProvenanceSetup.extractIncludedFilenames(prodPolicyFile,
                                                             repos))

        find = "lsst10-mysql.paf database/dc3a.paf platform/abecluster.paf".split()
        find += "mops.paf IPSD.paf".split()
        find += "IPSD/01-sliceInfo_policy.paf IPSD/02-symLink_policy-abe.paf IPSD/03-imageInput0_policy.paf IPSD/12-isr0_policy.paf IPSD/14-calibAndBkgdExposureOutput_policy.paf".split()

        self.assertEquals(len(files), len(find))

        for file in find:
            self.assert_(file in files, "Failed to file file: "+file)

    def testExtractPipeFiles(self):
        files = set(ProvenanceSetup.extractPipelineFilenames("IPSD",
                                                             prodPolicyFile,
                                                             repos))
        find = "mops.paf IPSD.paf".split()
        find += "IPSD/01-sliceInfo_policy.paf IPSD/02-symLink_policy-abe.paf IPSD/03-imageInput0_policy.paf IPSD/12-isr0_policy.paf IPSD/14-calibAndBkgdExposureOutput_policy.paf".split()
        self.assertEquals(len(files), len(find))

        for file in find:
            self.assert_(file in files, "Failed to file file: "+file)


__all__ = "ProvenanceSetupTestCase".split()

if __name__ == "__main__":
    unittest.main()
