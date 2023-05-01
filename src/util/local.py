"""
This module provides utilities for local development and testing.
"""

from pathlib import Path
from os import environ

import configparser
import toml
from snowflake.snowpark import Session
import logging

snowsql_config_file_mapping = {
"accountname" : "account",
"username" : "user",
"password" : "password",
"rolename" : "role",
"dbname" : "database",
"schemaname" : "schema",
"warehousename" : "warehouse"
}

def add_import(session:Session,package):
    from snowflake.snowpark._internal import utils
    if utils.PLATFORM == "XP":
        import sys, os, zipfile
        IMPORT_DIR = sys._xoptions["snowflake_import_directory"]
        lib = os.path.basename(package.__path__[-1][0:-1 * len(package.__name__)])
        end = (-1 * len(package.__name__) - 1)
        lib = os.path.basename(package.__path__[-1][:end])
        TARGER_FOLDER=f"/tmp/{lib}/"
        os.makedirs(TARGER_FOLDER,exist_ok=True)
        with zipfile.ZipFile(f'{IMPORT_DIR}{lib}', 'r') as zip_ref:
                zip_ref.extractall(TARGER_FOLDER)
        session.add_import(TARGER_FOLDER)
    else:
        session.add_import(package.__path__[-1], package.__name__)


      

def get_session(use_properties_file:bool=False,environment="dev", app_config_path:Path = Path.cwd().joinpath("app.toml"),query_tag=None)->Session:
    """
    Returns a session object
    """
    config_dict={}
    if use_properties_file:
        config_dict.update(get_dev_config(environment,app_config_path))
    else:
        config_dict.update(get_env_var_config())
    session = Session.builder.configs(config_dict).create()
    logging.info(config_dict)
    session.query_tag=query_tag
    return session

def get_env_var_config() -> dict:
    """
    Returns a dictionary of the connection parameters using the SnowSQL CLI
    environment variables.
    """
    try:
        return {
            "user":      environ.get("SNOWSQL_USER"),
            "password":  environ.get("SNOWSQL_PWD"),
            "account":   environ.get("SNOWSQL_ACCOUNT"),
            "role":      environ.get("SNOWSQL_ROLE"),
            "warehouse": environ.get("SNOWSQL_WAREHOUSE"),
            "database":  environ.get("SNOWSQL_DATABASE"),
            "schema":    environ.get("SNOWSQL_SCHEMA"),
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
        config_path = app_config["snowsql_config_path"]
        if not config_path:
            config_path = Path.home().joinpath(".snowsql/config")
        config.read(config_path)
        section_name = app_config["snowsql_connection_name"]
        session_config = config["connections" + ("." + section_name if section_name else "") ]
        session_config_dict = {
            snowsql_config_file_mapping[k.lower()]: v.strip('"') for k, v in session_config.items()
        }
        session_config_dict.update({ k:v for k,v in app_config.get(environment).items() if v})  # type: ignore
        return session_config_dict
    except Exception as exc:
        raise EnvironmentError(
            "Error creating snowpark session - be sure you've logged into "
            "the SnowCLI and have a valid app.toml file",
        ) from exc
