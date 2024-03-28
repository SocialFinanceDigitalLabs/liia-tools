from dagster import Config


class FileConfig(Config):
    filename: str
    name: str
