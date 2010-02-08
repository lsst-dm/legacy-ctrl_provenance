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
        
    def record(self, filename):
        """
        send the contents of the given file to the provenance store.
        """
        msg = 'called "abstract" Provenance.record'
        if self._logger:
            self._logger.log(Log.FATAL, msg)
        raise RuntimeError(msg)
