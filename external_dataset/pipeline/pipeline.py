import logging

import requests
from fs import open_fs

logger = logging.getLogger()


def retrieve_dataset(uri, location):
    response = get_dataset(uri)
    return write_dataset(response["filename"], response["response"], location)


def get_dataset(uri):
    response = requests.get(uri)

    filename = response.url.split("/")[-1]

    if response.status_code == 200:
        return {"response": response, "filename": filename}
    else:
        logger.exception("External request did not complete successfully")


def write_dataset(filename, response, location):
    response.raise_for_status()

    try:
        with open_fs(location) as home_fs:
            for chunk in response.iter_content(chunk_size=8192):
                home_fs.writebytes(filename, chunk)
            home_fs.close()
            return "success"
    except:
        logger.exception("Error writing file")
        return "error"
