name = "fady"
version = "1.0"

authors = ["ArtFx TD gang"]

requires = [
    "vray",
    "aces",
]


def commands():
    """
    Set the environment variables for Houdini
    """

    # DMNK TOOLS
    env.HOUDINI_PATH.append("C:/Users/etudiant/Documents/houdini18.5/DMNK-Tools-master")

    # MOPS
    env.HOUDINI_PATH.append("C:/Users/etudiant/Documents/houdini18.5/MOPS")

    # rvu packages
    env.HOUDINI_PACKAGE_DIR.append("C:/Users/etudiant/Documents/houdini18.5/packages")
