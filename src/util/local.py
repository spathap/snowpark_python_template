"""
This module provides utilities for local development and testing.
"""

from snowflake.snowpark import Session

from pathlib import Path
from os import environ

import configparser
import toml


def get_env_var_config() -> dict:
    """
    Returns a dictionary of the connection parameters using the SnowSQL CLI
    environment variables.
    """
    try:
        return {
            "user": environ["SNOWSQL_USER"],
            "password": environ["SNOWSQL_PWD"],
            "account": environ["SNOWSQL_ACCOUNT"],
            # "role": environ["SNOWSQL_ROLE"],
            "warehouse": environ["SNOWSQL_WAREHOUSE"],
            "database": environ["SNOWSQL_DATABASE"],
            "schema": environ["SNOWSQL_SCHEMA"],
        }
    except KeyError as exc:
        raise KeyError(
            "ERROR: Environment variable for Snowflake Connection not found. "
            + "Please set the SNOWSQL_* environment variables"
        ) from exc


def get_dev_config(
    environment: str = "dev",
    app_config_path: Path = Path.cwd().joinpath("app.toml"),
) -> dict:
    """
    Returns a dictionary of the connection parameters using the app.toml
    in the project root.
    """
    try:
        app_config = toml.load(app_config_path)
        config = configparser.ConfigParser(inline_comment_prefixes="#")
        config.read(app_config["snowsql_config_path"])
        session_config = config["connections." + app_config["snowsql_connection_name"]]
        session_config_dict = {
            k.replace("name", ""): v.strip('"') for k, v in session_config.items()
        }
        session_config_dict.update(app_config.get(environment))  # type: ignore
        return session_config_dict
    except Exception as exc:
        raise EnvironmentError(
            "Error creating snowpark session - be sure you've logged into "
            "the SnowCLI and have a valid app.toml file",
        ) from exc


def add_import(session:Session,package):
    from snowflake.snowpark._internal import utils
    if utils.is_in_stored_procedure():
        import sys, os, zipfile
        IMPORT_DIR = sys._xoptions["snowflake_import_directory"]
        EXT=".zip"
        idx = package.__path__[-1].rfind(EXT) 
        if idx == -1:
            EXT = ".py"
            idx = package.__path__[-1].rfind(".py")
        if idx == -1:
            raise Exception("Cannot .py .zip in package import")
        end = idx + len(EXT)
        path_to_lib=package.__path__[-1][:end]
        lib = os.path.basename(path_to_lib)
        if EXT=='.zip':
            TARGET_FOLDER=f"/tmp/{lib}"
            os.makedirs(TARGET_FOLDER,exist_ok=True)
            with zipfile.ZipFile(f'{IMPORT_DIR}{lib}', 'r') as zip_ref:
                    zip_ref.extractall(TARGET_FOLDER)
            session.add_import(TARGET_FOLDER)
        elif EXT == ".py":
            session.add_import(path_to_lib)
    else:
        session.add_import(package.__path__[-1], package.__name__)