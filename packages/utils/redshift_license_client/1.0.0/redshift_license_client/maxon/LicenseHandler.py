import os
import subprocess

from .License import License, LicenseList
from .UserInfo import UserInfo

MAXON_MX1_PATH = r"C:\Program Files\Maxon\Tools\mx1.exe"


def _maxon_single_command(parameters):
    cmds = [MAXON_MX1_PATH] + parameters
    maxonLoginProcess = subprocess.Popen(
        cmds,
        cwd=os.path.dirname(MAXON_MX1_PATH),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    response, responseError = maxonLoginProcess.communicate()
    print(f"Response: {response}")
    print(f"Response error: {responseError}")
    maxonLoginProcess.kill()
    return response, responseError


def maxon_login(user, password):
    response, responseError = _maxon_single_command(
        ["user", "login", "-u", user, "-p", password]
    )
    if len(responseError) > 0:
        return False
    if response.startswith("Successfully logged in"):
        return True
    return False


def maxon_logout():
    response, responseError = _maxon_single_command(["user", "logout"])
    if len(responseError) > 0:
        return False
    if response.startswith("user logged out\r\n"):
        return True
    return False


def maxon_userInfo():
    response, responseError = _maxon_single_command(["user", "info"])
    if len(responseError) > 0:
        return None
    return UserInfo.from_user_text(response)


def maxon_license_release(licenseName):
    response, responseError = _maxon_single_command(["license", "release", licenseName])
    if len(responseError) > 0:
        return False
    if response.startswith("Successfully released"):
        return True
    return False


def maxon_license_assign(licenseName):
    response, responseError = _maxon_single_command(["license", "assign", licenseName])
    if len(responseError) > 0:
        return False
    if response.startswith("Successfully assigned"):
        return True
    return False


def maxon_license_list():
    response, responseError = _maxon_single_command(["license", "list"])

    licenses = response.split("\n")

    licenselist = LicenseList()
    if len(licenses) > 2:
        for licenseText in licenses[2:]:
            if licenseText:
                licenselist.add_license(License().from_user_text(licenseText))

    return licenselist
