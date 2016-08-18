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

from lsst.pex.policy import Policy
from lsst.pex.logging import Log


class ProvenanceRecorder(object):
    """
    an abstract interface for recording prodution-level policy data as
    provenance into a particular database.  A provenance consumer
    instance (usually a DatabaseConfigurator, from ctrl.orca) will
    instantiate a subclass that is wired for that particular
    provenance store. 
    """

    def __init__(self, logger=None, fromSub=False):
        """
        As this class is abstract, it should only be executed from a
        subclass's constructor, in which case fromSub should be set to
        True.  
        @param logger    a logger to use for messages.  This will be
                            passed to each recorder.  If null, a
                            logger will be created as needed.
        @param fromSub   set this to True to indicate that it is being called
                            from a subclass constructor.  If False (default),
                            an exception will be raised under the assumption
                            that one is trying instantiate it directly.
        """
        # subclasses may wish to use a different logger name
        if not logger:
            logger = Log.getDefaultLog()
        self._logger = Log(logger, "provenance")

        if not fromSub:
            raise RuntimeError("Attempt to instantiate abstract class, " +
                               "ProvenanceRecorder; see class docs")

    def recordEnv(self):
        """
        Record the software and/or hardware environment.
        """
        self._logger.log(Log.DEBUG,
                         "no implementation for recording environment")

    def record(self, filename):
        """
        send the contents of the given file to the provenance store.
        """
        msg = 'called "abstract" Provenance.record'
        if self._logger:
            self._logger.log(Log.FATAL, msg)
        raise RuntimeError(msg)
