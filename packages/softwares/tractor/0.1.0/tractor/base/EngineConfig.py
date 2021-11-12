"""
This module provides an interface for reading the engine config files.
"""

import os, ast

# this is tractor's default postgresql port; changing this is likely to have no
# effect since the port is typically defined in the stock db.config file anyway.
# it only exists here if file happens to be edited by a site and the port
# entry is inadvertently removed.
DB_DEFAULT_PORT = 9876 
DB_DEFAULT_DATABASE_NAME = "tractor"

DB_CONFIG_FILENAME = "db.config"
DB_STARTUP = "DBStartup"
DB_USE_EXISTING = "DBUseExisting"
DB_INIT = "DBInit"
DB_SHUTDOWN = "DBShutdown"
DB_DATA_DIR = "DBDataDir"
DB_CONN = "DBConnection"
DB_ARCHIVING = "DBArchiving"
DB_DEBUG = "DBDebug"
DB_AUTO_UPGRADE = "DBAutoUpgrade"
DB_CONFIG_VARS = (
    DB_STARTUP,
    DB_USE_EXISTING,
    DB_AUTO_UPGRADE,
    DB_INIT,
    DB_SHUTDOWN,
    DB_CONN,
    DB_ARCHIVING,
    DB_DATA_DIR
    )

TRACTOR_CONFIG_FILENAME = "tractor.config"
TRACTOR_ENGINE_OWNER = "EngineOwner"
TRACTOR_DATA_DIR_MACRO = "${TractorDataDirectory}"
TRACTOR_DATA_DIRECTORY = "TractorDataDirectory"
TRACTOR_CONFIG_VARS = (
    TRACTOR_DATA_DIRECTORY,
    TRACTOR_ENGINE_OWNER,
)

class EngineConfigError(Exception):
    """A class for raising module-specific exceptions."""
    pass

class EngineConfig(object):
    """This class manages the reading of (possibly multiple) config files to report
    effective engine configuration values."""
    def __init__(self, configDir, installDir):
        self.configDir = configDir
        self.installDir = installDir
        self.dbConfig = self.readDbConfigFile()
        self.tractorConfig = self.readTractorConfigFile()
                
    def tractorDefaultConfigFilename(self):
        """Return the full path to the default tractor config file."""
        return os.path.join(self.installDir, "config", TRACTOR_CONFIG_FILENAME)

    def tractorSiteConfigFilename(self):
        """Return the full path to the site tractor config file."""
        return os.path.join(self.configDir, TRACTOR_CONFIG_FILENAME)

    def dbDefaultConfigFilename(self):
        """Return the full path to the default database config file."""
        return os.path.join(self.installDir, "config", DB_CONFIG_FILENAME)

    def dbSiteConfigFilename(self):
        """Return the full path to the site database config file."""
        return os.path.join(self.configDir, DB_CONFIG_FILENAME)

    def tractorDataDir(self):
        """Returns the full path to the tractor data directory."""
        return self.tractorConfig[TRACTOR_DATA_DIRECTORY]

    def tractorEngineOwner(self):
        """Returns intented owner of tractor process."""
        return self.tractorConfig[TRACTOR_ENGINE_OWNER]

    def pgDataDir(self):
        """Returns the full path to the directory storing the postgres configuration and tables."""
        pgDataDir = self.dbConfig[DB_DATA_DIR].replace(TRACTOR_DATA_DIR_MACRO, self.tractorDataDir())
        return pgDataDir

    def pgBaseDir(self):
        """Returns path to base postgresql (not data) directory.  Assumed to be relative to this script."""
        return os.path.join(self.installDir, "lib", "psql")

    def pgBinDir(self):
        """Returns path to postgresql bin directory.  Assumed to be relative to this script."""
        return os.path.join(self.pgBaseDir(), "bin")

    def pgLibDir(self):
        """Returns path to postgresql bin directory.  Assumed to be relative to this script."""
        return os.path.join(self.pgBaseDir(), "lib")

    def pgLogFilename(self):
        """Returns path to postgresql log file."""
        return os.path.join(self.pgDataDir(), "logfile")

    def isDbDebug(self):
        """Returns true if db.config is configured for debug mode."""
        return self.dbConfig.get(DB_DEBUG, False)
        
    def doDbArchiving(self):
        """Returns true if db.config is configured for archiving jobs."""
        return self.dbConfig.get(DB_ARCHIVING, False)
        
    def doDbInit(self):
        """Returns true if system is configured to initialized non-existent postgresql database."""
        return self.dbConfig.get(DB_INIT, False)
        
    def doDbUseExisting(self):
        """Returns true if system is configured to use a running postgresql server."""
        return self.dbConfig.get(DB_USE_EXISTING, False)
        
    def doDbStartup(self):
        """Returns true if system is configured to start up the postgresql server."""
        return self.dbConfig.get(DB_STARTUP, False)
        
    def doDbShutdown(self):
        """Returns true if system is configured to stop the postgresql server at system shutdown."""
        return self.dbConfig.get(DB_SHUTDOWN, False)
        
    def doDbAutoUpgrade(self):
        """Returns true if system is configured to automatically upgrade the schema."""
        return self.dbConfig.get(DB_AUTO_UPGRADE, True)
        
    def dbHostname(self):
        """Returns hostname on which database server should run."""
        return self.dbConfig[DB_CONN].get("host", "localhost")
        
    def dbPort(self):
        """Returns effective port."""
        return self.dbConfig[DB_CONN].get("port", DB_DEFAULT_PORT)

    def dbDatabaseName(self):
        """Returns name of database according to config file, with logical default."""
        return self.dbConfig[DB_CONN].get("dbname", DB_DEFAULT_DATABASE_NAME)

    def readConfigFile(self, filename):
        """Read the specified config file and return the results in a dictionary."""
        try:
            f = open(filename)
            configContents = f.read()
            f.close()
        except (IOError, OSError) as err:
            raise EngineConfigError("Unable to read config file %s: %s" % (filename, str(err)))
        # convert to python dictionary
        try:
            configDict = ast.literal_eval(configContents)
        except (ValueError, SyntaxError) as err:
            raise EngineConfigError("Problem evaluating config file %s: %s" % (filename, str(err)))
        return configDict
        
    def readTractorConfigFile(self):
        """Surmize the tractor configuration by first reading the default tractor.config file, and then overlaying
        values as set in the site-defined tractor.config file."""
        tractorConfig = self.readConfigFile(self.tractorDefaultConfigFilename())
        if os.path.exists(self.tractorSiteConfigFilename()):
            tractorConfig.update(self.readConfigFile(self.tractorSiteConfigFilename()))
        # check existence of important config vars
        for key in TRACTOR_CONFIG_VARS:
            if key not in tractorConfig:
                raise EngineConfigError("%s is not defined in config files %s nor %s" \
                      % (key, self.tractorDefaultConfigFilename(), self.tractorSiteConfigFilename()))
        return tractorConfig
            
    def readDbConfigFile(self):
        """Surmize the db configuration by first reading the default db.config file, and then overlaying
        values as set in the site-defined db.config file."""
        dbConfig = self.readConfigFile(self.dbDefaultConfigFilename())
        if os.path.exists(self.dbSiteConfigFilename()):
            dbConfig.update(self.readConfigFile(self.dbSiteConfigFilename()))
        # check existence of important config vars
        for key in DB_CONFIG_VARS:
            if key not in dbConfig:
                raise EngineConfigError("%s is not defined in config files %s nor %s" \
                      % (key, self.dbDefaultConfigFilename(), self.dbSiteConfigFilename()))
        return dbConfig
