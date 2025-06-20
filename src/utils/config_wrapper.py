import os
from os import getcwd

from dotenv import load_dotenv, set_key, find_dotenv

class ConfigWrapper:
    project_folder = None

    def __init__(self):
        self.project_folder = os.path.join(
            getcwd(),
            ".."
        )

        load_dotenv()

    def getenv(self, key):
        return os.getenv(key, False)

    def setenv(self, key, value):
        dotenv_path = find_dotenv()
        return set_key(dotenv_path, key, value)

ConfigWrapper()