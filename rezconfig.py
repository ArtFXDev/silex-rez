import os
import platform

is_linux = platform.system() == "Linux"

###############################################################################
# Paths
###############################################################################
# rez_install_dir = os.path.dirname(shutil.which("rez"))
# rez_root = os.path.abspath(os.path.join(rez_install_dir, "../../../../.."))
root_packages_path = [
    r"D:\rez\dev_packages",
    r"C:\rez\packages",
    r"\\192.168.2.112\rez\packages",
]

if is_linux:
    root_packages_path = [
        "~/packages",
        "/mnt/prod.silex.artfx.fr/rez/packages"
    ]

# The path that Rez will locally install packages to when rez-build is used
local_packages_path = "~/packages" if is_linux else r"C:\rez\packages"

# The path that Rez will deploy packages to when rez-release is used. For
# production use, you will probably want to change this to a site-wide location.
# release_packages_path = r"\\multifct\tools\rez\packages\artfx"
release_packages_path = r"\\prod.silex.artfx.fr\rez\packages\releases"


# Loop through subdirectories recursively that contain a .rez file
def child_packages(root):
    if not os.path.isdir(root):
        return
    directory_content = os.listdir(root)
    if ".rez" in directory_content:
        yield root
    else:
        return
    for content in directory_content:
        if content.startswith("_"):
            continue
        package_path = os.path.join(root, content)
        if os.path.isdir(package_path):
            for child_path in child_packages(package_path):
                yield child_path


packages_path = []
for path in root_packages_path:
    packages_path.append(path)
    packages_path.extend(list(child_packages(path)))

# Remove duplicates
packages_path = list(dict.fromkeys(packages_path))

os.environ["REZ_PACKAGES_PATH"] = os.pathsep.join(packages_path)

###############################################################################
# Package Resolution
###############################################################################

# Override platform values from Platform.os and arch.
# This is useful as Platform.os might show different
# values depending on the availability of lsb-release on the system.
# The map supports regular expression e.g. to keep versions.

platform_map = {
    "os": {
        r"windows-10(.*)": r"windows-10",  # windows-10.x.x -> windows-10
        r"Linux-(.*)": r"Linux",  # Linux-x.x -> Linux
        r"Ubuntu-14.\d": r"Ubuntu-14",  # Any Ubuntu-14.x      -> Ubuntu-14
        r"CentOS Linux-(\d+)\.(\d+)(\.(\d+))?": r"CentOS-\1.\2",  # Centos Linux-X.Y.Z -> CentOS-X.Y
    },
    "arch": {
        "x86_64": "64bit",
        "amd64": "64bit",
    },
}


###############################################################################
# Caching
###############################################################################

# Cache resolves to memcached, if enabled. Note that these cache entries will be
# correctly invalidated if, for example, a newer package version is released that
# would change the result of an existing resolve.
resolve_caching = False

# Cache package file reads to memcached, if enabled. Updated package files will
# still be read correctly (ie, the cache invalidates when the filesystem
# changes).
cache_package_files = False

# Cache directory traversals to memcached, if enabled. Updated directory entries
# will still be read correctly (ie, the cache invalidates when the filesystem
# changes).
cache_listdir = False

# The size of the local (in-process) resource cache. Resources include package
# families, packages and variants. A value of 0 disables caching; -1 sets a cache
# of unlimited size. The size refers to the number of entries, not byte count.
resource_caching_maxsize = 0

# Where temporary files go. Defaults to appropriate path depending on your
# system - for example, *nix distributions will probably set this to "/tmp". It
# is highly recommended that this be set to local storage, such as /tmp.
tmpdir = None

# Uris of running memcached server(s) to use as a file and resolve cache. For
# example, the uri "127.0.0.1:11211" points to memcached running on localhost on
# its default port. Must be either null, or a list of strings.
memcached_uri = []

# Bytecount beyond which memcached entries are compressed, for cached package
# files (such as package.yaml, package.py). Zero means never compress.
memcached_package_file_min_compress_len = 16384

# Bytecount beyond which memcached entries are compressed, for cached context
# files (aka .rxt files). Zero means never compress.
memcached_context_file_min_compress_len = 1

# Bytecount beyond which memcached entries are compressed, for directory listings.
# Zero means never compress.
memcached_listdir_min_compress_len = 16384

# Bytecount beyond which memcached entries are compressed, for resolves. Zero
# means never compress.
memcached_resolve_min_compress_len = 1


###############################################################################
# Package Caching
#
# Note: "package caching" refers to copying variant payloads to a path on local
# disk, and using those payloads instead. It is a way to avoid fetching files
# over shared storage, and is unrelated to memcached-based caching of resolves
# and package definitions as seen in the "Caching" config section.
#
###############################################################################

# Whether a package is relocatable or not, if it does not explicitly state with
