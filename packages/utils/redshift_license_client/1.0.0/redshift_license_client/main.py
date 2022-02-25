import sys

from .ioUtils import (
    clean_active_license_key,
    create_config_folder,
    get_active_license_key,
    save_active_license_key,
    server_add_license,
    server_get_license_info,
    server_get_license_key,
    server_list_licenses,
    server_release_license_key,
    server_remove_license,
)
from .maxon.License import MAXON_LICENSE
from .maxon.LicenseHandler import (
    maxon_license_assign,
    maxon_license_list,
    maxon_license_release,
    maxon_login,
    maxon_logout,
    maxon_userInfo,
)

LICENSE_SERVER_ADDR = ("192.168.2.111", 8888)
# LICENSE_SERVER_ADDR = ("172.17.0.2", 8888)
# LICENSE_SERVER_ADDR = ("127.0.0.1", 8888)


def activate_license():

    activeLicenseKey = get_active_license_key()

    if not activeLicenseKey:
        activeLicenseKey = server_get_license_key(LICENSE_SERVER_ADDR)

    save_active_license_key(activeLicenseKey)

    user_info = maxon_userInfo()
    print(f"User info: {user_info}")

    if user_info is None:
        licenseInfo = server_get_license_info(LICENSE_SERVER_ADDR, activeLicenseKey)
        # print(licenseInfo)
        if not maxon_login(licenseInfo["USER"], licenseInfo["PASSWORD"]):
            raise RuntimeError("Could not login to maxon server")

    if not maxon_license_list()[MAXON_LICENSE["STUDENT"]].active:
        if not maxon_license_assign(MAXON_LICENSE["STUDENT"]):
            raise RuntimeError("Unable to assign license")

    print("License aquried")


def release_license():
    activeLicenseKey = get_active_license_key()

    maxon_license_release(MAXON_LICENSE["STUDENT"])

    maxon_logout()

    server_release_license_key(LICENSE_SERVER_ADDR, activeLicenseKey)

    clean_active_license_key()

    print("License released")


def add_license(user, password, instance):
    return server_add_license(LICENSE_SERVER_ADDR, user, password, instance)


def remove_license(licenseKey):
    return server_remove_license(LICENSE_SERVER_ADDR, licenseKey)


def list_licenses():
    licenses = server_list_licenses(LICENSE_SERVER_ADDR)
    for license in licenses:
        # print(f"{license['KEY']}: {license['USER']} {license['PASSWORD']} {license['INUSE']} {license['NODE']}")
        print(
            f"{license['KEY']}: {license['USER']} {license['INUSE']} {license['NODE']}"
        )


def main():

    create_config_folder()

    if len(sys.argv) > 1:
        if sys.argv[1] == "start":
            activate_license()
            return
        elif sys.argv[1] == "stop":
            release_license()
            return
        elif sys.argv[1] == "add":
            if not len(sys.argv) == 5:
                print("add <user> <password> <instance>")
                return
            add_license(sys.argv[2], sys.argv[3], sys.argv[4])
            return
        elif sys.argv[1] == "rm":
            remove_license(sys.argv[2])
            return
        elif sys.argv[1] == "list":
            list_licenses()
            return
        else:
            print("Unknown command. start/stop")
    print("Not enough or too many arguments.")


if __name__ == "__main__":
    main()
