from lsst.pex.policy import Policy
from ProvenanceRecorder import ProvenanceRecorder

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

    def addProductionPolicy(self, filename):
        """
        add a policy file to record via the interested PolicyRecorders.
        Typically, the file will contain production-level policy data.
        @param filename   the full path to the policy file
        """
        self._pfiles.append(filename)

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
        self._cmdTmpls.append( (cmd, list(args), path) )

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

