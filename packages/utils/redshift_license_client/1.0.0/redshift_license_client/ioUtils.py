from logging import config
import os
import socket
import json
import platform

# Config ---------------------

def config_folder_path():
    appdata = os.getenv("LOCALAPPDATA")
    return os.path.join(appdata, "redshift_license")

def active_license_file_path():
    return os.path.join(config_folder_path(), "active.rslic")

def create_config_folder():
    config_path = config_folder_path()
    if not os.path.exists(config_path):
        os.makedirs(config_path)

def save_active_license_key(licenseKey):
    with open(active_license_file_path(), "wt") as license:
        license.write(licenseKey)
        license.flush()

def get_active_license_key():
    if not os.path.exists(active_license_file_path()):
        return ""
    with open(active_license_file_path(), "rt") as license:
        return license.read()

def clean_active_license_key():
    with open(active_license_file_path(), "wt") as license:
        license.write("")
        license.flush()

# -----------------------------

# Server communication --------

def communicate_server(addr, data, recvBufferSize=4096):
    retData = b""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(addr)
        sock.send(data)
        retData = sock.recv(recvBufferSize)
    return retData

def server_get_license_key(addr):
    request = {"CMD": "GET_LIC_KEY", "NODE": platform.node()}
    response = json.loads( communicate_server( addr, json.dumps(request).encode() ).decode() )
    if "SUCCESS" in response.keys():
        if response["SUCCESS"]:
            return response["KEY"]
        else:
            return None
    return None

def server_get_license_info(addr, licenseKey):
    request = {"CMD": "GET_LIC_INFO", "KEY": licenseKey}
    response = json.loads( communicate_server( addr, json.dumps(request).encode() ).decode() )
    if "SUCCESS" in response.keys():
        if response["SUCCESS"]:
            return response["INFO"]
        else:
            return None
    return None

def server_release_license_key(addr, licenseKey):
    request = {"CMD": "RELEASE_LIC", "KEY": licenseKey}
    response = json.loads( communicate_server( addr, json.dumps(request).encode() ).decode() )
    if "SUCCESS" in response.keys():
        return response["SUCCESS"]
    return None

def server_add_license(addr, user, password, instance):
    request = {"CMD": "ADD_LIC", "USER": user, "PASSWORD": password, "INSTANCE": instance}
    response = json.loads( communicate_server( addr, json.dumps(request).encode() ).decode() )
    if "SUCCESS" in response.keys():
        return response["SUCCESS"]
    return None

def server_remove_license(addr, licenseKey):
    request = {"CMD": "REMOVE_LIC", "KEY": licenseKey}
    response = json.loads( communicate_server( addr, json.dumps(request).encode() ).decode() )
    if "SUCCESS" in response.keys():
        return response["SUCCESS"]
    return None

def server_list_licenses(addr):
    request = {"CMD": "LIST_LIC"}
    response = json.loads( communicate_server( addr, json.dumps(request).encode() ).decode() )
    if "SUCCESS" in response.keys():
        if response["SUCCESS"]:
            return response["LICENSES"]
    return None


# -----------------------------