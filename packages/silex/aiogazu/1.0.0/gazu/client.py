import shutil
from typing import Any

import aiohttp

from .__version__ import __version__

from .exception import (
    TooBigFileException,
    NotAuthenticatedException,
    NotAllowedException,
    MethodNotAllowedException,
    ParameterException,
    RouteNotFoundException,
    ServerErrorException,
    UploadFailedException,
)


class KitsuClient(object):
    def __init__(self, host, ssl_verify=True):
        self.tokens = {"access_token": "", "refresh_token": ""}
        self.host = host
        self.event_host = host

    @property
    def headers(self):
        headers = {"User-Agent": "CGWire Gazu %s" % __version__}
        if "access_token" in self.tokens and self.tokens["access_token"]:
            headers["Authorization"] = "Bearer %s" % self.tokens["access_token"]
        return headers


def create_client(host):
    return KitsuClient(host)


default_client = None
try:
    # Little hack to allow json encoder to manage dates.
    # requests.models.complexjson.dumps = functools.partial(
    # json.dumps, cls=CustomJSONEncoder
    # )
    host = "http://gazu.change.serverhost/api"
    default_client = create_client(host)
except Exception:
    print("Warning, running in setup mode!")


async def host_is_up(client=default_client) -> bool:
    """
    Returns:
        True if the host is up.
    """
    try:
        await get("", client=client)
    except Exception:
        return False

    return True


async def host_is_valid(client=default_client) -> bool:
    """
    Check if the host is valid by simulating a fake login.
    Returns:
        True if the host is valid.
    """
    if not await host_is_up(client):
        return False
    try:
        await post("auth/login", {"email": "", "password": ""})
    except Exception as exc:
        return type(exc) == ParameterException

    return True


def get_host(client=default_client) -> str:
    """
    Returns:
        Host on which requests are sent.
    """
    return client.host


def get_api_url_from_host(client=default_client) -> str:
    """
    Returns:
        Zou url, retrieved from host.
    """
    return client.host[:-4]


def set_host(new_host, client=default_client) -> str:
    """
    Returns:
        Set currently configured host on which requests are sent.
    """
    client.host = new_host
    return client.host


def get_event_host(client=default_client):
    """
    Returns:
        Host on which listening for events.
    """
    return client.event_host or client.host


def set_event_host(new_host, client=default_client):
    """
    Returns:
        Set currently configured host on which listening for events.
    """
    client.event_host = new_host
    return client.event_host


def set_tokens(new_tokens, client=default_client) -> dict:
    """
    Store authentication token to reuse them for all requests.

    Args:
        new_tokens (dict): Tokens to use for authentication.
    """
    client.tokens = new_tokens
    return client.tokens


def make_auth_header(client=default_client) -> dict:
    """
    Returns:
        Headers required to authenticate.
    """
    headers = {"User-Agent": "CGWire Gazu %s" % __version__}
    if "access_token" in client.tokens:
        headers["Authorization"] = "Bearer %s" % client.tokens["access_token"]
    return headers


def url_path_join(*items) -> str:
    """
    Make it easier to build url path by joining every arguments with a '/'
    character.

    Args:
        items (list): Path elements
    """
    return "/".join([item.lstrip("/").rstrip("/") for item in items])


def get_full_url(path, client=default_client) -> str:
    """
    Args:
        path (str): The path to integrate to host url.

    Returns:
        The result of joining configured host url with given path.
    """
    return url_path_join(get_host(client), path)


async def get(path, json_response=True, params=None, client=default_client) -> Any:
    """
    Run a get request toward given path for configured host.

    Returns:
        The request result.
    """
    async with aiohttp.ClientSession(headers=client.headers) as session:
        async with session.get(get_full_url(path, client), params=params) as response:
            check_status(response.status, path)

            if json_response:
                return await response.json()
            else:
                return await response.text()


async def post(path, data, client=default_client) -> Any:
    """
    Run a post request toward given path for configured host.

    Returns:
        The request result.
    """
    async with aiohttp.ClientSession(headers=client.headers) as session:
        async with session.post(get_full_url(path, client), json=data) as response:
            check_status(response.status, path)
            return await response.json()


async def put(path, data, client=default_client) -> Any:
    """
    Run a put request toward given path for configured host.

    Returns:
        The request result.
    """
    async with aiohttp.ClientSession(headers=client.headers) as session:
        async with session.post(get_full_url(path, client), json=data) as response:
            check_status(response.status, path)
            return await response.json()


async def delete(path, params=None, client=default_client) -> Any:
    """
    Run a get request toward given path for configured host.

    Returns:
        The request result.
    """
    async with aiohttp.ClientSession(headers=client.headers) as session:
        async with session.get(get_full_url(path, client), params=params) as response:
            check_status(response.status, path)
            return await response.text()


def check_status(status_code, path):
    """
    Raise an exception related to status code, if the status code does not
    match a success code. Print error message when it's relevant.

    Args:
        request (Request): The request to validate.

    Returns:
        int: Status code

    Raises:
        ParameterException: when 400 response occurs
        NotAuthenticatedException: when 401 response occurs
        RouteNotFoundException: when 404 response occurs
        NotAllowedException: when 403 response occurs
        MethodNotAllowedException: when 405 response occurs
        TooBigFileException: when 413 response occurs
        ServerErrorException: when 500 response occurs
    """
    if status_code == 404:
        raise RouteNotFoundException(path)
    elif status_code == 403:
        raise NotAllowedException(path)
    elif status_code == 400:
        # TODO: Get the additional informations from the server
        # text = request.json().get("message", "No additional information")
        raise ParameterException(path)
    elif status_code == 405:
        raise MethodNotAllowedException(path)
    elif status_code == 413:
        raise TooBigFileException(
            "%s: You send a too big file. "
            "Change your proxy configuration to allow bigger files." % path
        )
    elif status_code in [401, 422]:
        raise NotAuthenticatedException(path)
    elif status_code in [500, 502]:
        # TODO: Get the stacktrace from the server
        # try:
        # stacktrace = request.json().get(
        # "stacktrace", "No stacktrace sent by the server"
        # )
        # message = request.json().get("message", "No message sent by the server")
        # print("A server error occured!\n")
        # print("Server stacktrace:\n%s" % stacktrace)
        # print("Error message:\n%s\n" % message)
        # except Exception:
        # print(request.text)
        raise ServerErrorException(path)
    return status_code


async def fetch_all(path, params=None, client=default_client) -> Any:
    """
    Args:
        path (str): The path for which we want to retrieve all entries.

    Returns:
        list: All entries stored in database for a given model. You can add a
        filter to the model name like this: "tasks?project_id=project-id"
    """
    return await get(url_path_join("data", path), params=params, client=client)


async def fetch_first(path, params=None, client=default_client) -> Any:
    """
    Args:
        path (str): The path for which we want to retrieve the first entry.

    Returns:
        dict: The first entry for which a model is required.
    """
    entries = await get(url_path_join("data", path), params=params, client=client)
    if len(entries) > 0:
        return entries[0]
    else:
        return None


async def fetch_one(model_name, id, client=default_client) -> Any:
    """
    Function dedicated at targeting routes that returns a single model
    instance.

    Args:
        model_name (str): Model type name.
        id (str): Model instance ID.

    Returns:
        dict: The model instance matching id and model name.
    """
    return await get(url_path_join("data", model_name, id), client=client)


async def create(model_name, data, client=default_client) -> Any:
    """
    Create an entry for given model and data.

    Args:
        model (str): The model type involved
        data (str): The data to use for creation

    Returns:
        dict: Created entry
    """
    return await post(url_path_join("data", model_name), data, client=client)


async def update(model_name, model_id, data, client=default_client) -> Any:
    """
    Update an entry for given model, id and data.

    Args:
        model (str): The model type involved
        id (str): The target model id
        data (dict): The data to update

    Returns:
        dict: Updated entry
    """
    return await put(url_path_join("data", model_name, model_id), data, client=client)


async def upload(
    path, file_path, data={}, extra_files=[], client=default_client
) -> Any:
    """
    Upload file located at *file_path* to given url *path*.

    Args:
        path (str): The url path to upload file.
        file_path (str): The file location on the hard drive.

    Returns:
        Response: Request response object.
    """
    files = _build_file_dict(file_path, extra_files)
    files.update(data)
    async with aiohttp.ClientSession(headers=client.headers) as session:
        async with session.post(get_full_url(path, client), data=files) as response:
            print(response)
            check_status(response.status, path)
            result = await response.json()
            if "message" in result:
                raise UploadFailedException(result["message"])
            return result


def _build_file_dict(file_path, extra_files):
    files = {"file": open(file_path, "rb")}
    i = 2
    for file_path in extra_files:
        files["file-%s" % i] = open(file_path, "rb")
        i += 1
    return files


def download(path, file_path, client=default_client) -> Any:
    """
    Download file located at *file_path* to given url *path*.

    Args:
        path (str): The url path to download file from.
        file_path (str): The location to store the file on the hard drive.

    Returns:
        Response: Request response object.

    """
    url = get_full_url(path, client)
    with client.session.get(
        url, headers=make_auth_header(client=client), stream=True
    ) as response:
        with open(file_path, "wb") as target_file:
            shutil.copyfileobj(response.raw, target_file)
        return response


def get_file_data_from_url(url, full=False, client=default_client):
    """
    Return data found at given url.
    """
    if not full:
        url = get_full_url(url)
    response = requests.get(
        url,
        stream=True,
        headers=make_auth_header(client=client),
    )
    check_status(response, url)
    return response.content


def import_data(model_name, data, client=default_client):
    """
    Args:
        model_name (str): The data model to import
        data (dict): The data to import
    """
    return post("/import/kitsu/%s" % model_name, data, client=client)


async def get_api_version(client=default_client) -> Any:
    """
    Returns:
        str: Current version of the API.
    """
    version = await get("", client=client)
    return version["version"]


async def get_current_user(client=default_client) -> Any:
    """
    Returns:
        dict: User database information for user linked to auth tokens.
    """
    user = await get("auth/authenticated", client=client)
    return user["user"]
