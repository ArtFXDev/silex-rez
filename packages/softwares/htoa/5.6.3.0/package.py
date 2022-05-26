name = "htoa"
version = "5.6.3.0"

tools = [
    "ArnoldLicenseManager",
    "Arnold",
    "hick",
    "ADPClientService",
    "kick",
    "maketx",
    "noice",
    "oiiotool",
    "oslc",
    "oslinfo",
]

requires = [
    "houdini-18.5.596"
]

def commands():
    env.PATH.prepend(r"{root}/htoa-5.6.3.0_ra766b1f_houdini-18.5.596/htoa-5.6.3.0_ra766b1f_houdini-18.5.596/scripts/bin")
    env.HOUDINI_PATH.prepend(r"{root}/htoa-5.6.3.0_ra766b1f_houdini-18.5.596/htoa-5.6.3.0_ra766b1f_houdini-18.5.596")
