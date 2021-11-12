"""Definition of all the tq commands that perform an operation on a list
of jobs returned from the database."""

import re, os, sys

import rpg.listutil as listutil
import rpg.stringutil as stringutil
import rpg.timeutil as timeutil
import rpg.CmdLineTool as CmdLineTool

import tractor.base.EngineClient as EngineClient
from .. import tq

__all__ = (
    "PingCmd",
    )

class PingCmd(CmdLineTool.CmdLineTool, tq.EngineClientMixin):
    """Ping the engine."""
    usage = "ping"
    description = "Verify the engine is reachable."
    
    def execute(self):
        """Ping the engine."""
        engineClient = self.engineClient()
        try:
            engineClient.open()
        except EngineClient.OpenConnError as err:
            raise tq.TqError(str(err))
        except EngineClient.LoginError as err:
            raise tq.TqError(str(err))
        result = engineClient.ping()
        if self.parent.opts.debug:
            print(result)
        print("Engine %s:%s is reachable as user %s with session id %s." % \
              (engineClient.hostname, engineClient.port, engineClient.user, engineClient.tsid))
        if self.parent.opts.logout or self.parent.opts.noSaveSession:
            engineClient.close()

class DBReconnectCmd(CmdLineTool.CmdLineTool, tq.EngineClientMixin):
    """Reestablish (close + open) the engine's database connections."""
    usage = "dbreconnect"
    description = "Signal engine to reestablish its connections with its database server."""
    
    def execute(self):
        """Signal engine to reestablish its connections with its database server."""
        engineClient = self.engineClient()
        try:
            engineClient.open()
        except EngineClient.OpenConnError as err:
            raise tq.TqError(str(err))
        except EngineClient.LoginError as err:
            raise tq.TqError(str(err))
        try:
            result = engineClient.dbReconnect()
            if self.parent.opts.debug:
                print(result)
            print("Requested engine %s:%s to reestablish database connections." % \
                  (engineClient.hostname, engineClient.port))
        except EngineClient.TransactionError as err:
            raise tq.TqError(str(err))
        if self.parent.opts.logout or self.parent.opts.noSaveSession:
            engineClient.close()

class QueueStatsCmd(CmdLineTool.CmdLineTool, tq.EngineClientMixin):
    """Display the engine's queue statistics."""
    usage = "queuestats"
    description = "Display the engine's queue statistics."""
    
    def execute(self):
        """Request queue statistics from engine."""
        engineClient = self.engineClient()
        try:
            engineClient.open()
        except EngineClient.OpenConnError as err:
            raise tq.TqError(str(err))
        except EngineClient.LoginError as err:
            raise tq.TqError(str(err))
        try:
            result = engineClient.queueStats()
        except EngineClient.TransactionError as err:
            raise tq.TqError(str(err))

        import pprint
        pprint.pprint(result)
        
        if self.parent.opts.logout or self.parent.opts.noSaveSession:
            engineClient.close()


class ReloadConfigCmd(CmdLineTool.CmdLineTool, tq.EngineClientMixin):
    """Notify engine to reload configuration files."""
    usage = "reloadconfig"
    description = "Notify engine to reload configuration files."

    options = [
        CmdLineTool.BooleanOption("--blade", help="reload blade.config"),
        CmdLineTool.BooleanOption("--crews", help="reload crews.config"),
        CmdLineTool.BooleanOption("--limits", help="reload limits.config"),
        CmdLineTool.BooleanOption("--tractor", help="reload tractor.config")
        ] + CmdLineTool.CmdLineTool.options
    
    def execute(self):
        """Notify engine to reload configuration files."""
        engineClient = self.engineClient()
        try:
            engineClient.open()
        except EngineClient.OpenConnError as err:
            raise tq.TqError(str(err))
        except EngineClient.LoginError as err:
            raise tq.TqError(str(err))
        results = []
        try:
            if self.opts.crews:
                results.append(engineClient.reloadCrewsConfig())
            if self.opts.blade:
                results.append(engineClient.reloadBladeConfig())
            if self.opts.limits:
                results.append(engineClient.reloadLimitsConfig())
            if self.opts.tractor:
                results.append(engineClient.reloadTractorConfig())
            if not self.opts.crews and not self.opts.blade and \
               not self.opts.limits and not self.opts.tractor:
                results.append(engineClient.reloadAllConfigs())
                
        except EngineClient.TransactionError as err:
            raise tq.TqError(str(err))

        import pprint
        for result in results:
            if result:
                pprint.pprint(result)
        
        if self.parent.opts.logout or self.parent.opts.noSaveSession:
            engineClient.close()
