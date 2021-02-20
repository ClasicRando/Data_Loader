from typing import List, Callable, Any
from dbfread.dbf import DBF
import re
from datetime import datetime, date
from keywords import keywords
from unidecode import unidecode
from decimal import Decimal
from numpy import format_float_positional
from configparser import ConfigParser
import psycopg2
import cx_Oracle as cx
import pyodbc
import sqlite3
import mysql.connector as mysql_connector
from functools import partial
from dataclasses import dataclass
from pandas import DataFrame


column_name_clean_ops: List[Callable] = [
    lambda x: unidecode(x).strip().upper(),
    lambda x: keywords[x] if x in keywords else x,
    lambda x: x.replace("#", "NO"),
    lambda x: re.sub(r"[^A-Za-z0-9]+", "_", x),
    lambda x: re.sub(r"_{2,}", "_", x),
    lambda x: re.sub(r"^(\d)", r"A\1", x)
]
table_name_clean_ops: List[Callable] = [
    lambda x: unidecode(x).strip().upper(),
    lambda x: keywords[x] if x in keywords else x,
    lambda x: re.sub(r"[^A-Za-z0-9]+", "_", x),
    lambda x: re.sub(r"_{2,}", "_", x),
    lambda x: re.sub(r"^(\d)", r"A\1", x)
]
connection_parameters = ["host", "user", "password", "dbname"]
columns_needed = ["Max Len", "Min Len", "Column Name Formatted", "Column Type"]


@dataclass
class AnalyzeResult:
    code: int
    num_records: int = -1
    column_stats: DataFrame = DataFrame()


def get_db_connection(ini_path: str, ini_section: str) -> Any:
    """
    Gets a database connection object based upon the details in the ini file specified

    Connection object can be of any type so no specific type or Union specified. A few parameters
    are needed to connect so before attempting to connect to the db, a check if run and if any
    required parameter is not found the function raises an exception with the missing parameters.
    If the ini section is not included in the file a KeyError will be raised and if the ini section
    does not correspond to a supported DB dialect then an Exception will be raised

    Parameters
    ----------
    ini_path : str
        string path to the credentials file
    ini_section :
        section/header in the ini file to find the db credentials

    Returns
    -------
    Connection object
    """
    config = ConfigParser()
    config.read(ini_path)
    db_credentials = config[ini_section]
    section = ini_section.upper()

    if section == "SQLITE":
        if "host" not in db_credentials:
            raise KeyError(f"DB parameter needed (host) is not available in the INI file")
    else:
        missing_credentials = [p for p in connection_parameters if p not in db_credentials]
        if missing_credentials:
            raise KeyError(
                f"DB parameter(s) needed ({','.join(missing_credentials)}) "
                f"are not available in the INI file"
            )

    if section == "POSTGRESQL":
        return psycopg2.connect(
            f"host={db_credentials['host']} user={db_credentials['user']} "
            f"password={db_credentials['password']} dbname={db_credentials['dbname']}"
        )
    elif section == "ORACLE":
        return cx.connect(
            db_credentials['user'],
            db_credentials['password'],
            f"{db_credentials['host']}"
            f"{':' + db_credentials['port'] if 'port' in db_credentials else ''}"
            f"/{db_credentials['service']}"
        )
    elif section == "MYSQL":
        return mysql_connector.connect(
            host=db_credentials['host'],
            user=db_credentials['user'],
            password=db_credentials['password'],
            database=db_credentials['dbname']
        )
    elif section == "SQLSERVER":
        return pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={db_credentials['host']}"
            f"{',' + db_credentials['port'] if 'port' in db_credentials else ''};"
            f"DATABASE={db_credentials['dbname']};"
            f"UID={db_credentials['user']};"
            f"PWD={db_credentials['password']}"
        )
    elif section == "SQLITE":
        return sqlite3.connect(db_credentials['host'])
    else:
        raise Exception("Database Dialect misspelled or not supported")


def find_encoding(path: str, file_type: str) -> str:
    """
    Tries to read the given file with 'utf8' then 'cp1252' to find the encoding

    If both encoding types raise a UnicodeDecodeError then the function raise an Exception

    Parameters
    ----------
    path : str
        Path to the DBF or FLAT file
    file_type : str
        Type of data file (DBF or FLAT)
    Returns
    -------
    'utf8' or 'cp1252' depending on the encoding of the file
    """
    i = 0
    try:
        if file_type == "FLAT":
            with open(path, mode="r", encoding="utf8") as f:
                for i, _ in enumerate(f):
                    pass
        elif file_type == "DBF":
            for i, _ in enumerate(DBF(path, encoding="utf8")):
                pass
        return "utf8"
    except UnicodeDecodeError as ex:
        print(
            f"{ex} for {'line' if file_type == 'FLAT' else 'record'} {i + 1}."
            "Moving to CP1252 encoding"
        )
        try:
            if file_type == "FLAT":
                with open(path, mode="r", encoding="cp1252") as f:
                    for i, _ in enumerate(f):
                        pass
            elif file_type == "DBF":
                for i, _ in enumerate(DBF(path, encoding="cp1252")):
                    pass
            return "cp1252"
        except UnicodeDecodeError as ex2:
            print(f"{ex2} for {'line' if file_type == 'FLAT' else 'record'} {i + 1}")
            raise Exception(
                "Could not infer encoding of the file. Please input the encoding as a keyword"
                "argument to the FileLoader constructor"
            )


def clean_column_name(column_name: str) -> str:
    """ Cleans a column name to fit a given standard. Iterates over a set of operations """
    result = column_name
    for op in column_name_clean_ops:
        result = op(result)
    return result


def clean_table_name(table_name: str) -> str:
    """ Cleans a table name to fit a given standard. Iterates over a set of operations """
    result = table_name
    for op in table_name_clean_ops:
        result = op(result)
    return result


def convert_column(encoding: str, x: Any) -> str:
    """
    Function used to transform elements of a DataFrame to string based upon the type of object

    This is the default implementation of the function that a FileLoader will use if the user does
    not supply their own function

    Parameters
    ----------
    encoding : str
        encoding to decode bytes to string. Usually a static value passed in using 'partial' from
        functools
    x : Any
        an object of Any type stored in a DataFrame
    Returns
    -------
     string conversion of the values based upon it's type
    """
    if isinstance(x, str):
        return x
    elif isinstance(x, datetime):
        return x.strftime("%d-%b-%Y %r").replace("NaT", "")
    elif isinstance(x, date):
        return x.strftime("%d-%b-%Y").replace("NaT", "")
    elif isinstance(x, int):
        return str(x)
    elif isinstance(x, float):
        return format_float_positional(x).rstrip(".")
    elif isinstance(x, Decimal):
        return str(round(x, 2))
    elif isinstance(x, bytes):
        return x.decode(encoding)
    elif isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    else:
        return str(x)


utf8_convert = partial(convert_column, "utf8")


def len_b_str(value: Any) -> int:
    """ transforms a value into the length in bytes when encoded as UTF8 """
    return len(str(value).encode("utf8"))


def get_oracle_column_type(length: int) -> str:
    """
    Gets a column type for the max byte length of a column

    Parameters
    ----------
    length : int
        max byte length of the column

    Returns
    -------
    Name of the column type as str
    """
    if length <= 1000:
        return "VARCHAR2(1000)"
    elif length <= 4000:
        return "VARCHAR2(4000)"
    elif length > 4000:
        return "CLOB"


def get_postgresql_column_type(_):
    """ Always returns TEXT for Postgresql column type """
    return "TEXT"


def get_mysql_column_type(length: int) -> str:
    """
    Gets a column type for the max byte length of a column

    Parameters
    ----------
    length : int
        max byte length of the column

    Returns
    -------
    Name of the column type as str
    """
    if length <= 65535:
        return "TEXT"
    elif length <= 16777215:
        return "MEDIUMTEXT"
    elif 16777215 < length <= 4294967295:
        return "LONGTEXT"


def get_sqlserver_column_type(length: int) -> str:
    """
    Gets a column type for the max byte length of a column

    Parameters
    ----------
    length : int
        max byte length of the column

    Returns
    -------
    Name of the column type as str
    """
    if length <= 1000:
        return "VARCHAR(1000)"
    elif length <= 4000:
        return "VARCHAR(4000)"
    elif length > 4000:
        return "VARCHAR(MAX)"


def get_sqlite_column_type(_) -> str:
    """ Always returns TEXT for SQLite column type"""
    return "TEXT"


dialect_to_col_type = {
    "ORACLE": get_oracle_column_type,
    "POSTGRESQL": get_postgresql_column_type,
    "MYSQL": get_mysql_column_type,
    "SQLSERVER": get_sqlserver_column_type,
    "SQLITE": get_sqlite_column_type
}
