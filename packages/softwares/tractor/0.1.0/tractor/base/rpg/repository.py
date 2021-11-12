import os
import re
from io import StringIO
import configparser

import rpg.osutil
import rpg.inpututil
import rpg.uniquefile
import rpg.pathutil

__all__ = (
        'Error',
        'AbortLog',
        'NameConflict',
        'Repository',
        'SVNRepository',
        'PerforceRepository',
        'GITRepository',
        'fromConfig',
        )

__repositories__ = ['SVNRepository', 'PerforceRepository', 'GITRepository']

# ---------------------------------------------------------------------------

class Error(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return str(self.msg)

class AbortLog(Error):
    pass

class NameConflict(Error):
    pass

class CommandFailed(Error):
    def __init__(self, cmd, msg=None):
        Error.__init__(self, msg)
        self.cmd = cmd

    def __str__(self):
        if self.msg is None:
            return 'Errored running command: "%s"' % self.cmd
        else:
            return 'Errored running command: "%s": %s' % (self.cmd, self.msg)

class UnknownRepository(Error):
    def __init__(self, repository, msg=None):
        Error.__init__(self, msg)
        self.repository = repository

    def __str__(self):
        if self.msg is None:
            return 'Unknown repository: "%s"' % self.repository
        else:
            return 'Unknown repository: "%s": %s' % \
                    (self.repository, self.msg)


# ---------------------------------------------------------------------------

class Repository(object):
    """Abstract class to simplify interacting with repositories"""

    def __init__(self, workingPath,
            filenamePrefix='temp.',
            verbose=False,
            dryRun=False):
        """
        @param workingPath: the local working directory
        @param filenamePrefix: use this filename as the temp file prefix
        @param verbose: print out extra debugging info
        @param dryRun: if set, do not run any commands that commit any data
        """

        self.workingPath = workingPath
        self.filenamePrefix=filenamePrefix
        self.verbose = verbose
        self.dryRun = dryRun

    ########

    def getHeadRevision(self):
        raise NotImplementedError

    def getParentRevision(self, revision):
        raise NotImplementedError

    def getNearestTag(self, revision):
        raise NotImplementedError

    ########

    def commit(self, message=None, filenamePrefix=None):
        """
        check in the local working copy into the repository

        @param message: checks in the codw with the comment
        @param filenamePrefix: if set, overrides the one set in __init__
        """

        if not self.canCommit():
            # we don't need to run
            return

        if filenamePrefix is None:
            filenamePrefix = self.filenamePrefix

        messageFilename = self._input_log(filenamePrefix, message,
                self.initCommitMessage(),
                self.finalizeCommitMessage)

        self._runRepositoryCmd(self.getCommitCommand(messageFilename))

        # if we wrote a note, remove it
        if os.path.exists(messageFilename):
            os.remove(messageFilename)


    def canCommit(self):
        """overloadable method to determine if the specified project can
        be checked in"""

        raise NotImplementedError


    def initCommitMessage(self):
        return ''


    def finalizeCommitMessage(self, message):
        return message

    ########

    def tag(self, tagname, message=None, filenamePrefix=None, force=False):
        """
        mark the version as a tagged version

        @param tagname: name of the tag
        @param message: checks in the codw with the comment
        @param filenamePrefix: if set, overrides the one set in __init__
        @param force: force making the tag
        """

        if not force and not self.canTag(tagname):
            # tag already exists, error out
            raise NameConflict('tag already exists: %s' % tagname)

        if filenamePrefix is None:
            filenamePrefix = self.filenamePrefix

        messageFilename = self._input_log(filenamePrefix, message,
                self.initTagMessage(),
                self.finalizeTagMessage)

        self._runRepositoryCmd(self.getTagCommand(tagname, messageFilename,
            force=force))

        # if we wrote a note, remove it
        if os.path.exists(messageFilename):
            os.remove(messageFilename)


    def canTag(self, tagname):
        """overloadable method to determine if the specified project can
        be tagged"""
        raise NotImplementedError


    def initTagMessage(self):
        return ''


    def finalizeTagMessage(self, message):
        return message

    ########

    def export(self, destination, version=None, tagname=None):
        """
        copy files in the repository into a local directory without any of
        the manditory repository files

        @param destination: directory to output the branch to
        @param version: if set, export the specified version
        @param tagname: if set, export the specified tag, else, the trunk
        """
        self._runRepositoryCmd(
                self.getExportCommand(destination, version, tagname))


    def getExportCommand(destination, version, tagname):
        raise NotImplementedError

    ########

    def latestVersion(self):
        return max([v for f, v in self.filesAndVersions()])


    def files(self, version=None):
        """
        get the files of the local code from the repository

        @param version: list the files from a specific version
        """
        return [f for f, v in self.filesAndVersions(version)]


    def filesAndVersions(self, version=None):
        """
        overloadable method to get the files of the local code from the
        repository and what version they are

        @param version: list the files from a specific version
        """
        cmd = self.getFilesAndVersionsCommand(version)

        return [self.extractFileAndVersion(l) \
                for l in StringIO(self._runRepositoryCmdOutput(cmd))]


    def getFilesAndVersionsCommand(self, version):
        raise NotImplementedError


    def extractFileAndVersion(self, line):
        raise NotImplementedError

    ########

    def status(self):
        """overloadable method to get a string status of the local code"""

        return self._runRepositoryCmdOutput(self.getStatusCommand())


    def getStatusCommand(self):
        raise NotImplementedError

    ########

    def outOfDate(self):
        """overloadable method to get a string status of the local code"""

        return self._runRepositoryCmdOutput(self.getOutOfDateCommand())


    def getOutOfDateCommand(self):
        raise NotImplementedError

    ####

    def sync(self):
        if not self.canSync():
            # we don't need to run
            return

        self._runRepositoryCmd(self.getSyncCommand())

    def getSyncCommand(self):
        raise NotImplementedError

    def canSync(self):
        """overloadable method to determine if the specified project is
        out of date with the repository"""
        return bool(self.outOfDate())

    ########

    def _input_log(self, prefix, message, initMsg, finalizeMsg):
        initMsg = initMsg.rstrip()

        # create a uniquely named file
        f = None
        mode = 'r+'
        try:
            f, filename = rpg.uniquefile.mkuniqueobj(prefix, mode=mode)

            # if we specified a default message, write it in now
            if message is not None:
                f.write(finalizeMsg(message).rstrip())
            else:
                f.write(initMsg)
        finally:
            if f:
                f.close()

        ####
        # if we didn't specify a message, have the user edit one

        if message is None:
            # check if there was input written to the file
            def fileWritten():
                rpg.inpututil.edit(filename)
                f = None
                try:
                    f = open(filename)
                    value = finalizeMsg(f.read()).rstrip()
                finally:
                    if f:
                        f.close()

                # if input was entered, break out of the loop
                if value and value != initMsg:
                    f = None
                    try:
                        f = open(filename, 'w')
                        f.write(value)
                    finally:
                        if f:
                            f.close()

                    return True

            # note that the edit option doesn't do anything
            if not rpg.inpututil.confirm(
                    'Log message unchanged or not specified\n'
                    'a)bort, c)ontinue, e)dit\n',
                    responses={'a': False, 'c': True},
                    func=fileWritten):

                os.remove(filename)
                raise AbortLog('user canceled submission')

        ####

        return filename

    ####

    def _runRepositoryCmd(self, cmd):
        """run an arbitrary repository command"""

        cmd = 'cd %s && %s' % (self.workingPath, cmd)

        if self.verbose > 1:
            print(cmd)

        if not self.dryRun:
            errno = os.system(cmd)

            if errno:
                raise CommandFailed(cmd)

    ####

    def _runRepositoryCmdOutput(self, cmd):
        """run an arbitrary repository command"""

        cmd = 'cd %s && %s' % (self.workingPath, cmd)

        if self.verbose > 1:
            print(cmd)

        errno, stdout, stderr = rpg.osutil.runCommand(cmd)
        stderr = stderr.strip()

        if errno:
            raise CommandFailed(cmd, stderr)

        if self.verbose:
            s = stdout.strip()
            if s:
                print(s)

        return stdout

# ---------------------------------------------------------------------------

class SVNRepository(Repository):
    footer = '--This line, and those below, will be ignored--'

    def __init__(self, workingPath,
            projectURL=None,
            tagURL=None,
            svnPath='/usr/bin/env svn',
            **kwds):
        """
        @param workingPath: path to the local working directory
        @param projectURL: path to the project directory on the server
        @param tagURL: path to the tag directory on the server
        @param svnPath: path to the svn executable
        """

        super(SVNRepository, self).__init__(workingPath, **kwds)

        self.tagURL = tagURL
        self.svnPath = svnPath

        if projectURL is not None:
            self.projectURL = projectURL

            # error out if project url doesn't exist
            cmd = '%s info %s' % (self.svnPath, self.projectURL)
            self._runRepositoryCmd(cmd)
        else:
            cmd = '%s info %s' % (self.svnPath, workingPath)
            errno, stdout, stderr = rpg.osutil.runCommand(cmd)

            if errno:
                raise CommandFailed(cmd)

            for line in StringIO(stdout):
                if line.startswith('URL: '):
                    self.projectURL = line.strip().lstrip('URL: ')
                    break
            else:
                raise Error('Failed to look up subversion project url')

        # error out if tag url doesn't exist
        if self.tagURL is not None:
            cmd = '%s info %s' % (self.svnPath, self.tagURL)

            # ignore output
            self._runRepositoryCmdOutput(cmd)

    ########

    def getHeadRevision(self):
        return 'HEAD'

    def getParentRevision(self, revision):
        return revision + '^'

    ########

    def canCommit(self):
        """we can check in if any files have been modified"""
        return bool(self.status())


    def getCommitCommand(self, messageFilename):
        return '%s commit --file %s' % (self.svnPath, messageFilename)


    def initCommitMessage(self):
        s = self.status()
        return '\n%s\n\n%s' % (SVNRepository.footer, s)


    def finalizeCommitMessage(self, message):
        message = message.split(SVNRepository.footer)[0]
        return message

    ########

    def canTag(self, tagname):
        """we can tag a release if that tagname hasn't already been tagged"""

        # check to see if that path exists. if not, we can run
        url = self._getURL(tagname)
        cmd = '%s list %s' % (self.svnPath, url)

        if self.verbose > 1:
            print(cmd)

        errno, stdout, stderr = rpg.osutil.runCommand(cmd)

        # we can run if the command errored out
        return errno != 0


    def getTagCommand(self, tagname, messageFilename, force=False):
        if self.tagURL is None:
            raise Error('must specify the tagURL')

        return '%s copy --file %s %s %s/%s' % (
                self.svnPath,
                messageFilename,
                self.projectURL,
                self.tagURL,
                tagname)


    def finalizeTagMessage(self, message):
        return message.split(SVNRepository.footer.rstrip())[0]

    ####

    def getLogBetweenRevisions(self, startRevision, endRevision):
        raise NotImplementedError

    ####

    def getExportCommand(self, destination, version, tagname):
        url = self._getURL(tagname)
        if version is None:
            return '%s export %s %s' % (self.svnPath, url, destination)
        else:
            return '%s export -r %s %s %s' % \
                    (self.svnPath, version, url, destination)

    ####

    def getFilesAndVersionsCommand(self, version=None):
        if version is None:
            return '%s list -R -v -r HEAD %s' % \
                    (self.svnPath, self.workingPath)
        else:
            return '%s list -R -v -r %s %s' % \
                    (self.svnPath, version, self.workingPath)


    _fileAndVersionRegex = re.compile(
            r'\W*(?P<version>\d+)'                 # version
            r'\W+(?P<commiter>\w+)'                # committer
            r'\W+(?P<size>\d*)'                    # size
            r'\W+(?P<date>\w+\W+\d+\W+\d+(:\d+)?)' # date
            r'\W+(?P<filename>.*)'                 # filename
            )

    def extractFileAndVersion(self, line):
        m = self._fileAndVersionRegex.match(line)

        if not m:
            raise Error('unknown line: ' + line)

        filename = m.group('filename')
        version = int(m.group('version'))

        return (filename, version)


    ####

    def getStatusCommand(self):
        """check if any files have changed"""

        return '%s status -q %s' % (self.svnPath, self.workingPath)

    ####

    def getOutOfDateCommand(self):
        return '%s status -u -q %s' % (self.svnPath, self.workingPath)


    def outOfDate(self, *args, **kwds):
        result = super(SVNRepository, self).outOfDate(*args, **kwds)

        # the out of date files are marked with a '*', so filter on those
        # lines
        output = StringIO()
        for line in StringIO(result):
            if '*' in line:
                output.write(line)

        return output.getvalue()

    ####

    def getSyncCommand(self):
        return '%s update %s' % (self.svnPath, self.workingPath)

    ########

    def _getURL(self, tagname=None):
        if tagname is None:
            url = self.projectURL
        else:
            if self.tagURL is None:
                raise Error('must specify the tagURL')

            url = '%s/%s' % (self.tagURL, tagname)

        return url

# ---------------------------------------------------------------------------

class PerforceRepository(Repository):
    def __init__(self, workingPath,
            p4Path='/usr/bin/env p4',
            **kwds):
        super(PerforceRepository, self).__init__(workingPath, **kwds)

        self.p4Path = p4Path

    ########

    def canCommit(self):
        return bool(self.status())


    def getPerforceChangelist(self, changelist):
        cmd = '%s change -o' % self.p4Path
        if changelist != 'default':
            cmd += ' %d' % changelist

        return self._runRepositoryCmdOutput(cmd)


    def commit(self, message=None, filenamePrefix=None):
        if not self.canCommit():
            # we don't need to run
            return

        if filenamePrefix is None:
            filenamePrefix = self.filenamePrefix

        default_changelist = False
        changelists = {}
        regex = re.compile(r'edit (?:change (\d+))|(?:default change)')
        for line in StringIO(self.status()):
            m = regex.search(line)
            if m is None:
                raise Error('cannot extract changelist number')

            changelist = m.group(1)
            if changelist is None:
                default_changelist = True
            else:
                changelists[int(changelist)] = None

        changelists = list(changelists.keys())
        changelists.sort()

        ####

        def run(changelist):
            messageFilename = self._input_log(filenamePrefix, message,
                    self.getPerforceChangelist(changelist),
                    self.finalizeCommitMessage)

            self._runRepositoryCmd(
                    self.getCommitCommand(messageFilename, changelist))

            # if we wrote a note, remove it
            if os.path.exists(messageFilename):
                os.remove(messageFilename)

        ####

        if default_changelist:
            run('default')

        for changelist in changelists:
            run(changelist)


    def getCommitCommand(self, messageFilename, changelist=None):
        cmd = 'cat %s | %s submit -i' % (messageFilename, self.p4Path)
        if changelist != 'default':
            cmd += ' -c %d' % changelist

        return cmd

    ####

    def canTag(self, tagname):
        cmd = '%s files @%s' % (self.p4Path, tagname)

        try:
            return not bool(self._runRepositoryCmdOutput(cmd))
        except CommandFailed as e:
            # if the changelist doesn't exist, it raises this error
            if e.msg.startswith('Invalid changelist/client/label/date'):
                return True
            raise e


    def getTagCommand(self, tagname, messageFilename, force=False):
        return '%s tag -l %s ...' % (self.p4Path, tagname)

    ####

    def export(self, destination, version=None, tagname=None):
        """
        copy files in the repository into a local directory without any of
        the manditory repository files

        @param destination: directory to output the branch to
        @param version: if set, export the specified version
        @param tagname: if set, export the specified tag, else, the trunk
        """
        if version is not None and tagname is not None:
            raise Error('cannot specify version and tagname at the same time')

        # look up the perforce specific path info for the working path
        cmd = '%s dirs %s' % (self.p4Path, self.workingPath)
        if version is not None:
            cmd += '@' + str(version)
        elif tagname is not None:
            cmd += '@' + str(tagname)

        repo_dir = self._runRepositoryCmdOutput(cmd).strip()

        # perforce doesn't return an error code if we cannot find this info
        # so we have to brute force the error
        if repo_dir.endswith('no file(s) at that changelist number.'):
            raise CommandFailed(cmd)

        for path in self.files(version):
            dirname, basename = os.path.split(path)

            # strip off the perforce specific path from the file path
            dirname = rpg.pathutil.relativepath(repo_dir, dirname)
            dirname = os.path.join(destination, dirname)
            new_path = os.path.join(dirname, basename)

            if not os.path.exists(dirname):
                if self.verbose:
                    print('making directory:', dirname)

                if not self.dryRun:
                    os.makedirs(dirname)

            cmd = '%s print -o %s -q %s' % (self.p4Path, new_path, path)

            if version is not None:
                cmd += '@' + str(version)
            elif tagname is not None:
                cmd += '@' + str(tagname)

            self._runRepositoryCmd(cmd)

    ####

    def getFilesAndVersionsCommand(self, version):
        cmd = '%s files ...' % self.p4Path
        if version is not None:
            cmd += '@%s' % version

        return cmd + ' | grep -v " - delete change"'


    _fileAndVersionRegex = re.compile(
            r'(?P<filename>.*?)#\d+ - (?:\w+) change (?P<version>\d+) \(.*?\)'
            )

    def extractFileAndVersion(self, line):
        m = self._fileAndVersionRegex.match(line)

        if not m:
            raise Error('unknown line: ' + line)

        filename = m.group('filename')
        version = int(m.group('version'))

        return (filename, version)

    ####

    def getStatusCommand(self):
        """grabs any changelists"""

        return '%s opened ...' % self.p4Path

    ####

    def getOutOfDateCommand(self):
        return '%s sync -n ...' % self.p4Path


    def getSyncCommand(self):
        return '%s sync ...' % self.p4Path

# ---------------------------------------------------------------------------

class GITRepository(Repository):
    footer = '--This line, and those below, will be ignored--'

    def __init__(self, workingPath,
            gitPath='/usr/bin/env git',
            **kwds):
        super(GITRepository, self).__init__(workingPath, **kwds)

        self.gitPath = gitPath

    ########

    def getHeadRevision(self):
        return 'HEAD'

    def getParentRevision(self, revision):
        return revision + '^'

    ########

    def canCommit(self):
        """We can commit if any of the files have been modified"""
        return bool(self.status())

    def getCommitCommand(self, messageFilename):
        return '%s commit -a --file=%s' % (self.gitPath, messageFilename)

    def initCommitMessage(self):
        s = self.status()
        return (GITRepository.footer, s)

    def finalizeCommitMessage(self, message):
        message = message.split(GITRepository.footer)[0]
        return message

    ########

    def canTag(self, tagname):
        """we can tag a release if that tagname hasn't already been tagged"""
        cmd = '%s tag -l %s' % (self.gitPath, tagname)

        if self.verbose > 1:
            print(cmd)

        errno, stdout, stderr = rpg.osutil.runCommand(cmd)

        # we can run if the command errored out or there's no output.
        return errno != 0 or not stdout.strip()

    def getTagCommand(self, tagname, messageFilename, force=False):
        cmd = '%s tag -F %s ' % (self.gitPath, messageFilename)

        if force:
            cmd += '--force '

        return cmd + tagname

    def finalizeTagMessage(self, message):
        return message.split(SVNRepository.footer.rstrip())[0]

    ########

    def getNearestTag(self, revision):
        cmd = '%s describe %s' % (self.gitPath, revision)
        errno, stdout, stderr = rpg.osutil.runCommand(cmd)

        if errno != 0:
            raise MissingTag(stderr)

        return stdout.split('-', 1)[0].strip()

    ########

    def getLogBetweenRevisions(self, startRevision, endRevision):
        return self._runRepositoryCmdOutput(
            '%s log %s..%s' % (self.gitPath, startRevision, endRevision))

    ########

    def getExportCommand(self, destination, version=None, tagname=None):
        """
        copy files in the repository into a local directory without any of
        the manditory repository files

        @param destination: directory to output the branch to
        @param version: if set, export the specified version
        @param tagname: if set, export the specified tag, else, the trunk
        """
        if version is not None and tagname is not None:
            raise Error('cannot specify version and tagname at the same time')

        cmd = '%s archive --format=tar ' % self.gitPath
        if version is not None:
            cmd += version
        elif tagname is not None:
            cmd += tagname
        else:
            cmd += 'HEAD'

        return cmd + ' | tar -x -C %s' % destination

    ########

    def getStatusCommand(self):
        """"""
        return '%s status --porcelain --untracked-files=no' % self.gitPath

    def outOfDate(self):
        # We are only out of date if we are rejected due to a non-fast-forward.
        try:
            self._runRepositoryCmdOutput(
                '%s push -n' % self.gitPath)
        except CommandFailed as err:
            # Check if we were rejected, if so, then we could sync
            if 'non-fast-forward' in err.msg:
                return True
            else:
                raise
        else:
            return False

    def version(self, name):
        return self._runRepositoryCmdOutput(
            '%s rev-parse --short %s^{}' % (self.gitPath, name)).strip()

    def latestVersion(self):
        return self.version('HEAD')

    ########

    #def getSyncCommand(self):
    #    return 'cd %s && %s pull' (self.gitPath, self.workingPath)

# ---------------------------------------------------------------------------

def fromConfig(configFilename, workingPath, **kwds):
    """
    read repository info from a config file. For example:

    [SVNRepository]
    projectURL=http://subversion/foo/trunk
    tagURL=http://subversion/foo/tags

    """

    config = configparser.ConfigParser()
    config.optionxform = str # we want case sensitivity
    config.read(configFilename)

    # check to make sure only one repository type is defined
    if len(config.sections()) != 1:
        raise Error('only one repository type can be defined')

    repository = config.sections()[0]

    if repository not in __repositories__:
        raise Error('unknown repository: ' + repository)

    configOptions = dict(config.items(repository))

    # allow the config file to overload the kwds
    kwds.update(configOptions)

    try:
        return globals()[repository](workingPath, **kwds)
    except KeyError as e:
        raise UnknownRepository(repository, e)

# ---------------------------------------------------------------------------

def guessRepository(workingPath, verbose=0, **kwds):
    if verbose > 1:
        print('guessing repository')

    # for git and svn, we can just check if their data dirs exist.
    if os.path.exists(os.path.join(workingPath, '.git')):
        repository = GITRepository(workingPath, verbose=verbose, **kwds)
    elif os.path.exists(os.path.join(workingPath, '.svn')):
        repository = SVNRepository(workingPath, verbose=verbose, **kwds)
    else:
        # Otherwise for perforce, we need to see if we can get status.
        repository = PerforceRepository(workingPath, verbose=verbose, **kwds)
        try:
            # see if we can get the status
            repository.status()
        except Error:
            repository = None

    if verbose > 1:
        if repository:
            print('found:', repository)
        else:
            print('could not determine repository')

    return repository
