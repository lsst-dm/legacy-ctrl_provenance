from __future__ import absolute_import
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
import sys

from lsst.pex.policy import Policy
from lsst.pex.logging import Log
import lsst.pex.exceptions
from .ProvenanceRecorder import ProvenanceRecorder


class ProvenanceSetup(object):
    """
    a container for collecting and bring together provideres and
    consumers of provenance information in order to record provenance
    for a production run.

    This class is motivated by the need (in the ctrl_orca package) to
    have an abstract interface for recording provenance that hides the
    details of how to connect to the consumer of provenance and the
    specific data needed to do so.

    This class is intended to be used via a visitor pattern:  this
    container is passed to any class interested in receiving provenance
    information.  That class passes in an encapsulation of how to
    record the provenance as a ProvenanceRecorder instance and/or a 
    command line template (for recording on a remote platform).  
    """

    def __init__(self):

        # list of policy files to record via PolicyRecorders
        self._pfiles = []

        # the list of interested consumers, as PolicyRecorder instances
        self._consumers = []

        # the list of command line templates
        self._cmdTmpls = []

    def addProductionPolicyFile(self, filename):
        """
        add a policy file to record via the interested PolicyRecorders.
        Typically, the file will contain production-level policy data.
        @param filename   the full path to the policy file
        """
        self._pfiles.append(filename)

    def addAllProductionPolicyFiles(self, filename, repository=".",
                                    pipefile="workflow.pipeline.definition",
                                    logger=None):
        """
        add for recording the given production policy file along with all
        of the production policy files referred to inside.  

        After adding filename via addProductionPolicy(), it is opened 
        recursively to find all filenames mentioned via the policy 
        file include mechanism, which are also added (uniquely) to the 
        list.  Pipeline definition policy files (identified by pipefile)
        are not added.

        @param filename    the production policy file.  This is assumed to 
                              be a production-level policy file.  If it is
                              not, pipefile should be set accordingly.  This
                              filename should give the complete path to the
                              file.
        @param repository  the assumed policy repository directory where 
                              included policy files are to be found.  When
                              an include file is found, its name will be 
                              prepended with this path before being added.
        @param pipefile    the hierarchical policy name for a pipeline 
                              definition.  Any included policy filenames at 
                              this node or lower will not be added.
        @param logger      if provided, use this Log to record any
                              warnings about missing or bad files;
                              otherwise, problems are silently ignored.
        """
        filenames = ProvenanceSetup.extractIncludedFilenames(filename,
                                                             repository,
                                                             pipefile,
                                                             logger)
        self.addProductionPolicyFile(filename)
        for file in filenames:
            self.addProductionPolicyFile(os.path.join(repository, file))

    def getFiles(self):
        """
        return the list of production policy filenames that will get
        recorded.  
        """
        return list(self._pfiles)

    def addProductionRecorder(self, recorder):
        """
        register the desire to receive production-level provenance by 
        providing a ProvenanceRecorder instance to receive production policy
        data.  
        @param recorder   a ProvenanceRecorder instance that understands
                            how to connect to a provenance store,
                            database, or other consumer
        """
        if not isinstance(recorder, ProvenanceRecorder):
            raise TypeError("addProductionRecorder(): arg not a ProvenanceRecorder")
        self._consumers.append(recorder)

    def addWorkflowRecordCmd(self, cmd, args=None, path=None):
        """
        register the desire to receive workflow-level provenance by
        providing shell-executable command to record that
        provenance. The expectation is that this command will be
        executed on the remove execution platform when the workflow is
        launched. The cmd argument must not include any path as part of
        its name. The command must accept one or more arguments (after
        the provided args) representing filenames of policies to
        record. 
        @param cmd    the name of the command (without arguments) to
                        execute.
        @param args   the arguments to pass to the command (prior to
                        the list of policy files) 
        @param path   the path to the executable on the launch
                        platform.  This path allows the executable to
                        be copied over to the execution platform where
                        the workflow will run.  The basename is not
                        required to be the same as given in cmd.  
        """
        if args is None:
            args = []
        elif not isinstance(args, list):
            raise TypeError("addWorkflowRecordCmd: args: not a list")
        self._cmdTmpls.append((cmd, list(args), path))

    def getRecorders(self):
        """
        return the recorders that have been added to this setup thus far.
        """
        return list(self._consumers)

    def recordProduction(self):
        """
        record the production-level policy provenance to all
        interested databases. This will do this by looping through
        each ProvenanceRecorder that it has and call its record() 
        function.
        """
        for consumer in self._consumers:
            for file in self._pfiles:
                consumer.record(file)
            consumer.recordEnv()

    def getCmdPaths(self):
        """
        return the paths to the registered provenance-recording
        scripts. This is used to find the scripts that must be
        transfered to the remote workflow platform where they will be
        executed.

        The number of elements returned is the same as the outer list
        returned by getCmds().  If an element is non-None, then the
        script should be copied to the remote workflow platform.  
        """
        return map(lambda s: s[2], self._cmdTmpls)

    def getCmds(self):
        """
        Return the list of commands that should be run on each
        workflow platform to record provenance for that workflow. The
        returned value is a list of list of strings. Each element of
        the outer list represents the command to execute. The first
        element of the inner list is the name of the executable
        command (sans path); the remaining elements are the arguments
        to pass, in order. The expected algorithm for forming a
        complete command line for each command (i.i. element in the
        outer list) is: 
          1. prepend to the first element (of inner list) the
             directory path for the location of the executable on the
             remote platform. 
          2. append as extra arguments the pathnames to the policy
             files to record. 
          3. join and execute the command word list.
        """
        out = []
        for cmd in self._cmdTmpls:
            cl = [cmd[0]]
            cl.extend(cmd[1])
            out.append(cl)
        return out

    @staticmethod
    def extractIncludedFilenames(prodPolicyFile, repository=".",
                                 pipefile=None, logger=None):
        """
        extract all the filenames included, directly or indirectly, from the 
        given policy file.  When a repository is provided, included files will
        be recursively opened and searched.  The paths in the returned set
        will not include the repository directory.  Use pipefile to skip 
        the inclusion of pipeline policy files.

        @param prodPolicyFile   the policy file to examine.  This must
                                  be the full path to the file
        @param repository       the policy repository.  If None, the current
                                   directory is assumed.
        @param pipefile         the hierarchical policy name for a pipeline 
                                   definition.  Any included policy filenames 
                                   at this node or lower will not be added.
        @param logger           if provided, use this Log to record any
                                   warnings about missing or bad files;
                                   otherwise, problems are silently ignored.
        @return set   containing the unique set of policy filenames found,
                        including the given top file and the 
        """
        prodPolicy = Policy.createPolicy(prodPolicyFile, False)
        filenames = set([prodPolicyFile])
        ProvenanceSetup._listFilenames(filenames, prodPolicy, None, repository,
                                       pipefile, logger)
        filenames.discard(prodPolicyFile)
        return filenames

    @staticmethod
    def _listFilenames(fileset, policy, basename, repository, stopname=None,
                       logger=None):
        if stopname and basename and not stopname.startswith(basename):
            stopname = None
        for name in policy.names(True):
            fullname = basename and ".".join([basename, name]) or name
            if stopname and fullname == stopname:
                continue

            if policy.isFile(name):
                files = policy.getArray(name)
                for file in files:
                    file = file.getPath()
                    if file not in fileset:
                        fileset.add(file)
                        file = os.path.join(repository, file)
                        if not os.path.exists(file):
                            if logger:
                                logger.log(logger.WARN, "Policy file not found in repository: %s" % file)
                                continue
                        try:
                            if logger and logger.sends(Log.DEBUG):
                                logger.log(Log.DEBUG, "opening log file: %s"%file)
                            fpolicy = Policy.createPolicy(file, False)
                            ProvenanceSetup._listFilenames(fileset, fpolicy,
                                                           fullname, repository,
                                                           stopname, logger)
                        except lsst.pex.exceptions.Exception as ex:
                            if logger:
                                logger.log(Log.WARN, "problem loading %s: %s" % (file, str(ex)))
                            continue

            elif policy.isPolicy(name):
                pols = policy.getArray(name)
                for pol in pols:
                    ProvenanceSetup._listFilenames(fileset, pol, fullname,
                                                   repository, stopname, logger)

    @staticmethod
    def extractPipelineFilenames(wfname, prodPolicyFile, repository=".",
                                 logger=None):
        """
        extract all non-pipeline policy files in the given production
        policy file.
        @param wfname           the name of the workflow of interest
        @param prodPolicyFile   the production-level policy file
        @param repository       the policy repository
        @param logger           if provided, use this Log to record any
                                   warnings about missing or bad files;
                                   otherwise, problems are silently ignored.
        """
        prodPolicy = Policy.createPolicy(prodPolicyFile, False)

        out = []
        wfs = ProvenanceSetup._shallowPolicyNodeResolve("workflow", prodPolicy,
                                                        repository, logger)
        if not wfs:
            return out
        for wfp in wfs:
            if wfp is None:
                continue
            if not wfp.exists("shortName") or wfp.get("shortName") != wfname:
                continue

            pipes = ProvenanceSetup._shallowPolicyNodeResolve("pipeline",
                                                              wfp, repository)
            for pipe in pipes:
                if not pipe.exists("definition") or not pipe.isFile("definition"):
                    continue
                pipe = pipe.get("definition").getPath()
                out.append(pipe)
                pipe = os.path.join(repository, pipe)
                if not os.path.exists(pipe):
                    if logger:
                        logger.log(Log.WARN, "Policy file not found in repository: "+pipe)
                    continue
                out += list(ProvenanceSetup.extractIncludedFilenames(pipe,
                                                                     repository))

        return out

    @staticmethod
    def extractSinglePipelineFileNames(pipe, repository=".", logger=None):
        """
        extract all pipeline policy files in the given pipeline policy
        @param pipe             the pipeline of interest
        @param repository       the policy repository
        @param logger           if provided, use this Log to record any
                                   warnings about missing or bad files;
                                   otherwise, problems are silently ignored.
        """
        out = []
        if not pipe.exists("definition") or not pipe.isFile("definition"):
            return
        pipe = pipe.get("definition").getPath()
        out.append(pipe)
        pipe = os.path.join(repository, pipe)
        if not os.path.exists(pipe):
            if logger:
                logger.log(Log.WARN, "Policy file not found in repository: "+pipe)
            return
        out += list(ProvenanceSetup.extractIncludedFilenames(pipe, repository))
        return out

    @staticmethod
    def _shallowPolicyNodeResolve(pname, policy, repository, logger=None):
        if not policy.exists(pname):
            return []

        nodes = policy.getArray(pname)
        if policy.isFile(pname):
            for i in xrange(len(nodes)):
                try:
                    if not os.path.isabs(nodes[i]):
                        nodes[i] = os.path.join(repository, nodes[i])
                    nodes[i] = Policy.createPolicy(nodes[i], False)
                except lsst.pex.exceptions.Exception as ex:
                    if logger:
                        logger.log(Log.WARN, "problem finding/loading "+nodes[i])
                    nodes[i] = None

        return nodes
