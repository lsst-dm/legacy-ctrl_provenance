#!/usr/bin/env python
import lsst.ctrl.provenance.dc3 as dc3

import os.path
import optparse

from lsst.ctrl.provenance.ProvenanceSetup import ProvenanceSetup
from lsst.pex.logging import Log

usage = """usage: %prog --runid=<runid> --activityname=<actvityname> --platform=<platform> --dbrun=<dbrun> --dbglobal=<dbglobal> --activoffset=<activoffset> --runoffset=<runoffset> [policyfile]+"""
parser = optparse.OptionParser(usage)

parser.add_option("-r", "--runid", action="store", dest="runid", default=None)
parser.add_option("-a", "--activityname", action="store", dest="activityname", default=None)
parser.add_option("-p", "--platform", action="store", dest="platform", default=None)
parser.add_option("-R", "--dbrun", action="store", dest="dbrun", default=None)
parser.add_option("-G", "--dbglobal", action="store", dest="dbglobal", default=None)
parser.add_option("-v", "--activoffset", type="int", action="store", dest="activoffset", default=0)
parser.add_option("-o", "--runoffset", type="int", action="store", dest="runoffset", default=0)
parser.add_option("-w", "--localrepos", action="store", dest="localrepos", default=None)
#    arguments = "--runid=<runid> --activityname=<activityname> --platform=<platform> --dbrun=<dbrun> --dbglobal=<dbglobal> --activoffset=<activoffset> --runoffset=<runoffset>"

parser.opts = {}
parser.args = []

(parser.opts, parser.args) = parser.parse_args()

logger = Log(Log.getDefaultLog(), "PipelineProvenanceRecorder")

recorder = dc3.Recorder(parser.opts.runid, parser.opts.activityname, parser.opts.platform, parser.opts.dbrun, parser.opts.dbglobal, parser.opts.activoffset, parser.opts.runoffset, logger)

provSetup = ProvenanceSetup()
provSetup.addProductionRecorder(recorder)

for i in range(len(parser.args)):
    filename = os.path.join(parser.opts.localrepos, parser.args[i])
    provSetup.addProductionPolicyFile(filename)
provSetup.recordProduction()
