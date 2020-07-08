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

import os

from lsst.ctrl.provenance import ProvenanceRecorder
import eups
import hashlib
import re
import lsst.pex.exceptions
from lsst.pex.policy import Policy
from lsst.pex.logging import Log
from lsst.daf.persistence import DbStorage, LogicalLocation
from lsst.daf.base import DateTime

runid_incr = 2097152
activ_incr = 512


def _offsetToActivityId(runidx, activeidx):
    return runidx * runid_incr + activeidx * activ_incr


class Recorder(ProvenanceRecorder):
    """
    An implementation of the ProvenanceRecorder interface that understands
    how to record provenance to the DC3 database.

    To work, the user must have a $HOME/.lsst/db-auth.paf file installed
    and must have permission to write to the database.
    """

    def __init__(self, runId, activityName, platform, dbLoc, globalDbLoc,
                 activOffset=0, runOffset=None, logger=None):
        """
        Initialize a ProvenanceRecorder.
        @param runId            the unique production run ID
        @param activityName     a name for the activity that provenance is
                                  being recorded for.  On the launch platform
                                  this should be the name of the production
                                  run (not the runid).  On a workflow platform
                                  (where pipelines are run), this should be
                                  the name of the workflow.
        @param platform         a logical name for the platform where this
                                  class has been instantiated.  This is not
                                  typically a DNS name, but it can be.  This
                                  is usually the name from the platform policy.
        @param dbLoc            the URL representing the production run-
                                  specific database
        @param globalLoc        the URL representing the global database
                                  shared by all production runs.
        @param activOffset      the integer ID assigned to the current workflow
                                  by the orchestration layer which is unique
                                  to the runid.  On the launch platform, this
                                  should be zero.  On the workflow platforms,
                                  this is n for nth workflow listed in the
                                  production run policy file.
        @param runOffset        the integer ID assigned to this run (runId)
                                  by the database.  This should be None
                                  when instantiating from the launch platform.
                                  In this case, the run will be initialized
                                  to assign the runOffset (which can later
                                  be retrieved via getRunOffset()).  On
                                  workflow platforms the runOffset must be
                                  provided properly associate workflow
                                  provenance with the right production run.
        @param logger           a Log instance to use for messages
        """

        ProvenanceRecorder.__init__(self, logger, True)
        self._runid = runId
        self._activName = activityName
        self._platName = platform

        # the index for the this production run
        self._roffset = runOffset

        # the index for this activity (launch process or workflow)
        self._aoffset = activOffset

        self._rundb = DbStorage()
        self._rundb.setPersistLocation(LogicalLocation(dbLoc))
        self._globalLoc = LogicalLocation(globalDbLoc)
        self._globalDb = DbStorage()
        self._globalDb.setPersistLocation(self._globalLoc)

        self.initialize()

    def recordEnv(self):
        """
        an implementation of the ProvenanceRecorder API that records details
        about the environment (software, hardware, etc) where the current
        activity is running.
        """
        self.recordEnvironment()

    def record(self, filename):
        """
        an implementation of the ProvenanceRecorder API.  This will record
        the given policy filename.
        """
        self.recordPolicy(filename)

    def getRunOffset(self):
        """
        return the index offset for this run (as identified by its runid)
        that was assigned by the database.  None is returned if it has not
        yet been assigned.
        """
        return self._roffset

    def initialize(self):
        """
        add data to the provenance database that identifies this run (if
        necessary) and activity.  In particular, this will assign
        the runId's index offset (which can be retrieved afterward via
        getRunOffset()).
        """
        isOrch = self._roffset is None
        if isOrch:
            acttype = "launch"
            self.initProdRun()
        else:
            acttype = "workflow"

        self.initActivity(self._activName, acttype, self._platName)

    def queryRunOffset(self):
        """
        query the database to get the run offset for the current runid.
        None is returned if the runid has not been registered, yet.
        """
        self._globalDb.setRetrieveLocation(self._globalLoc)

        self._globalDb.startTransaction()
        self._globalDb.setTableForQuery("prv_Run")
        self._globalDb.outColumn("offset")
        self._globalDb.condParamString("runId", self._runid)
        self._globalDb.setQueryWhere("runId = :runId")
        self._globalDb.query()
        if not self._globalDb.next() or self._globalDb.columnIsNull(0):
            return None
        roffset = self._globalDb.getColumnByPosInt(0)
        self._globalDb.finishQuery()
        self._globalDb.endTransaction()

        self._globalDb.setPersistLocation(self._globalLoc)
        return roffset

    def initProdRun(self):
        """
        register the production run via its runid.  This will assign a
        run offset to this run.
        """
        if self._roffset is not None:
            raise lsst.pex.exceptions.RuntimeError("runId appears to already be registered")

        self._globalDb.setPersistLocation(self._globalLoc)

        self._globalDb.startTransaction()
        self._globalDb.setTableForInsert("prv_Run")
        self._globalDb.setColumnString("runId", self._runid)
        self._globalDb.insertRow()
        self._globalDb.endTransaction()

        self._roffset = self.queryRunOffset()
        if self._roffset is None:
            msg = "failed to register runid"
            self._logger.log(Log.WARN+5, msg)
            raise lsst.pex.exceptions.RuntimeError(msg)

    def initActivity(self, name, typen, platform):
        """
        register an activity (workflow, or other operation on a platform).
        """
        if self._roffset is None:
            raise lsst.pex.exceptions.NotFoundError("Unknown runid index (offset)")
        if self._aoffset is None:
            raise lsst.pex.exceptions.NotFoundError("Unknown activity index")

        activityId = _offsetToActivityId(self._roffset, self._aoffset)

        self._globalDb.startTransaction()
        self._globalDb.setTableForInsert("prv_Activity")
        self._globalDb.setColumnInt64("activityId", activityId)
        self._globalDb.setColumnString("type", typen)
        self._globalDb.setColumnString("name", name)
        self._globalDb.setColumnString("platform", platform)
        self._globalDb.insertRow()
        self._globalDb.endTransaction()

        self._policyFileId = activityId + 1
        self._policyKeyId = activityId + 1

    def recordEnvironment(self):
        """Record the software environment of the pipeline."""

        setupList = eups.Eups().listProducts(setup=True)
        # self._realRecordEnvironment(self._rundb, setupList)
        self._realRecordEnvironment(self._globalDb, setupList)

    def _realRecordEnvironment(self, db, setupList):
        db.startTransaction()

        id = _offsetToActivityId(self._roffset, self._aoffset) + 1
        for product in setupList:
            db.setTableForInsert("prv_SoftwarePackage")
            db.setColumnInt64("packageId", id)
            db.setColumnString("packageName", product.name)
            db.insertRow()

            db.setTableForInsert("prv_cnf_SoftwarePackage")
            db.setColumnInt64("packageId", id)
            db.setColumnString("version", product.version)
            db.setColumnString("directory", product.dir)
            db.insertRow()

            id += 1

        db.endTransaction()

    def recordPolicy(self, policyFile):
        """Record the contents of the given Policy as part of provenance."""

        md5 = hashlib.md5()
        f = open(policyFile, 'r')
        for line in f:
            md5.update(line)
        f.close()

        # self._realRecordPolicyFile(self._rundb, policyFile, md5)
        self._realRecordPolicyFile(self._globalDb, policyFile, md5)

        p = Policy.createPolicy(policyFile, False)
        for key in p.paramNames():
            type = p.getTypeName(key)
            val = p.str(key)  # works for arrays, too
            val = re.sub(r'\0', r'', val)  # extra nulls get included
            # self._realRecordPolicyContents(self._rundb, key, type, val)
            self._realRecordPolicyContents(self._globalDb, key, type, val)

            self._policyKeyId += 1

        self._policyFileId += 1

    def _realRecordPolicyFile(self, db, file, md5):
        db.startTransaction()

        db.setTableForInsert("prv_PolicyFile")
        db.setColumnInt64("policyFileId", self._policyFileId)
        db.setColumnString("pathname", file)
        db.setColumnString("hashValue", md5.hexdigest())
        db.setColumnInt64("modifiedDate",
                          DateTime(os.stat(file)[8] * 1000000000, DateTime.UTC).nsecs())
        db.insertRow()

        db.endTransaction()

    def _realRecordPolicyContents(self, db, key, type, val):
        db.startTransaction()
        db.setTableForInsert("prv_PolicyKey")
        db.setColumnInt64("policyKeyId", self._policyKeyId)
        db.setColumnInt64("policyFileId", self._policyFileId)
        db.setColumnString("keyName", key)
        db.setColumnString("keyType", type)
        db.insertRow()

        db.setTableForInsert("prv_cnf_PolicyKey")
        db.setColumnInt64("policyKeyId", self._policyKeyId)
        db.setColumnString("value", val)
        db.insertRow()

        db.endTransaction()
