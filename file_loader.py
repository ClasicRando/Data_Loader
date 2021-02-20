from pandas import DataFrame, read_excel, read_sql, read_csv, Series, merge
from typing import Generator, Callable, Any, Optional
from dbfread.dbf import DBF
from sqlalchemy import create_engine
import urllib
from util import (clean_column_name, clean_table_name, find_encoding, len_b_str, get_db_connection,
                  dialect_to_col_type, convert_column, AnalyzeResult, columns_needed)
from functools import partial
from psycopg2 import extras
import time
from os.path import abspath


class FileLoader:

    def __init__(self,
                 file_path: str,
                 file_type: str,
                 value_convert: Optional[Callable[[Any], str]] = None,
                 **kwargs):
        """
        Creates a FileLoader object to assist in the insertion of a data file into a DB

        Parameters
        ----------
        file_path : str
            Path to a data file as a string
        file_type : str
            category of data file passed. Currently supported types are: 1) FLAT (.txt,
            .csv, .tsv, etc.) 2) ACCDB (.accdb and .mdb) 3) DBF (.dbf) 4) XLSX (.xlsx)
        value_convert : (Optional) if the user wants to specify how values are to be converted
            to string objects to be inserted into the DB, the can do so here. The expected format is
            '(Any) -> str' as to account for Pandas inferring data types but always converting the
            DataFrame elements to string objects

        Keyword arguments
        -----------------
        separator : str
            delimited data's separator character. Defaults to ','
        qualifier : bool
            whether or not the data is qualified. Only accept data with '"' as the qualifier.
            Defaults to False
        encoding : str
            encoding of the text file or DBF. Defaults to 'utf8' and falls back on 'cp1252' if not
            'utf8'
        table_name : str
            name of the table to extract for an access database. Required for ACCDB
        sheet_name : str
            name of the sheet to extract for the excel file. Required for XLSX
        chunk_size : int
            chunksize parameter used for read_sql and read_csv. Number of records to read into
            memory at a given time. Does nothing if file_type not FLAT or ACCDB
        """
        self.path = abspath(file_path)
        self.file_type = file_type
        self.encoding = "utf8"
        self.chunk_size = 10000
        if self.file_type == "FLAT":
            self.separator = kwargs["separator"] if "separator" in kwargs else ","
            self.qualifier = kwargs["qualifier"] if "qualifier" in kwargs else False
            if "encoding" in kwargs:
                self.encoding = kwargs["encoding"]
            else:
                find_encoding(self.path, self.file_type)
            self.chunk_size = kwargs["chunk_size"] if "chunk_size" in kwargs else 10000
        elif self.file_type == "ACCDB":
            if "table_name" in kwargs:
                self.table_name = kwargs["table_name"]
            else:
                raise Exception(
                    "table_name in access file was not passed as keyword argument."
                    "Please pass a table name from the accdb to continue"
                )
            self.chunk_size = kwargs["chunk_size"] if "chunk_size" in kwargs else 10000
        elif self.file_type == "DBF":
            if "encoding" in kwargs:
                self.encoding = kwargs["encoding"]
            else:
                find_encoding(self.path, self.file_type)
        elif self.file_type == "XLSX":
            if "sheet_name" in kwargs:
                self.sheet_name = kwargs["sheet_name"]
            else:
                raise Exception(
                    "sheet_name not passed as keyword argument for excel file."
                    "Please pass a sheet name from the excel file to continue"
                )
        else:
            raise Exception(f"File type {file_type} is not supported or contains a typo")
        if not self.encoding:
            raise Exception(
                "Could not find encoding for file."
                "Find encoding and pass as keyword argument to constructor"
            )
        else:
            if value_convert is None:
                self.convert_column = partial(convert_column, self.encoding)
            elif not callable(value_convert):
                raise TypeError("value_convert parameter is expected to be callable")
            else:
                self.convert_column = value_convert

    def get_data(self) -> Generator[DataFrame, None, None]:
        """
        Method to generate the DataFrames from the given file/data

        Depending upon the file type, this will either be 1 DataFrame or a sequence of chunked
        DataFrame instances. By default the chunk sizes are 10000 records but that parameter can be
        passed into the constructor if the file_type supports the feature. If the file_type does
        not support chunk sizes then all the records will be loaded in at once into 1 DataFrame

        Returns
        -------
        Generated DataFrame instances
        """
        data = DataFrame()
        if self.file_type == "FLAT":
            data = read_csv(
                self.path,
                sep=self.separator,
                encoding=self.encoding,
                quoting=0 if self.qualifier else 3,
                dtype=object,
                keep_default_na=False,
                na_values=[""],
                # skip_blank_lines=False,
                chunksize=self.chunk_size
            )
        elif self.file_type == "ACCDB":
            conn_str = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + self.path
            conn_uri = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
            accdb_engine = create_engine(conn_uri)
            data = read_sql(
                f"select * from {self.table_name}",
                accdb_engine,
                chunksize=self.chunk_size
            )
        elif self.file_type == "DBF":
            data = DataFrame(iter(DBF(self.path, encoding=self.encoding)))
        elif self.file_type == "XLSX":
            data = read_excel(
                self.path,
                sheet_name=self.sheet_name,
                dtype=object,
                keep_default_na=False,
                na_values=[""],
            )
        if isinstance(data, DataFrame):
            yield data.fillna("").applymap(self.convert_column)
        else:
            for df in data:
                yield df.fillna("").applymap(self.convert_column)

    def analyze_file(self, dialect: str) -> AnalyzeResult:
        """
        Method to analyze the given file/data and produce column stats as well as loading parameters

        Parameters
        ----------
        dialect : str
            database dialect that is to be used for loading the data. Dictates the column types
            Only dialects currently supported are:
                1) Postgresql
                2) Oracle
                3) MySQL
                4) SQL Server
                5) SQLite

        Returns
        -------
        Dataclass that holds the result code, number of records and column stats (as a DataFrame)
        """
        b_max_len: Series = Series()
        b_min_len: Series = Series()
        num_records = 0
        try:
            for df in self.get_data():
                num_records += len(df.index)
                b_lens = df.applymap(len_b_str)
                if b_max_len.empty:
                    b_max_len = b_lens.max()
                    b_min_len = b_lens.min()
                else:
                    b_max_len = b_max_len.combine(b_lens.max(), max)
                    b_min_len = b_max_len.combine(b_lens.min(), min)
        except Exception as ex:
            print(ex)
            return AnalyzeResult(-1)
        b_min_len.name = "Min Len"
        df = merge(
            DataFrame(
                b_max_len,
                columns=["Max Len"]
            ),
            b_min_len,
            left_index=True,
            right_index=True
        )
        df["Column Name Formatted"] = df.index.map(clean_column_name)
        df["Column Type"] = df["Max Len"].map(dialect_to_col_type[dialect.upper()])
        return AnalyzeResult(1, num_records, df)

    def load_file(
            self,
            ini_path: str,
            ini_section: str,
            table_name: str,
            column_stats: Optional[DataFrame] = None) -> int:
        """
        Method to load the referenced file/data into the database specified by the ini file

        Parameters
        ----------
        ini_path : str
            path to the ini file contains the database credentials
        ini_section : str
            section header name of the ini to be used for the database connection. This is assumed
            to also be the database dialect
        table_name : str
            name used when inserting the records into the database
        column_stats : Optional[DataFrame]
            if you have already analyzed the file and have the DataFrame needed to provided column
            stats, you can provide this optional parameter. If nothing is provided then the
            'analyze_file' function is called to get the column stats. If a DataFrame is provided,
            the columns will be checked to ensure it meets the needs to provide the create table
            statement and an Exception will be raised if it does not pass the test

        Returns
        -------
        Number of records inserted. A negative result means an error occurred
        """
        dialect = ini_section.upper()
        connection = get_db_connection(ini_path, ini_section)
        cursor = connection.cursor()
        cleaned_table_name = clean_table_name(table_name)
        if column_stats is None:
            print("Getting Column Stats")
            result = self.analyze_file(dialect)
            if result.code != 1:
                return -1
            column_stats = result.column_stats
            print("Done getting Column Stats")
        if any([c not in column_stats.columns for c in columns_needed]):
            return -3
        try:
            data = self.get_data()
        except Exception as ex:
            print(ex)
            return -2
        if data is None:
            return -4
        data_types = list(map(
            lambda x: f"{x[0]} {x[1]}",
            column_stats.set_index("Column Name Formatted")["Column Type"].items()
        ))
        columns_names = ",".join(column_stats["Column Name Formatted"].to_list())
        create_sql = f"CREATE TABLE {cleaned_table_name}({','.join(data_types)})"

        insert_sql = ""
        if dialect == "POSTGRESQL":
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES %s"
        elif dialect == "ORACLE":
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join((f':{i + 1}' for i in range(len(column_stats.index))))})"
        elif dialect == "MYSQL":
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('%s' for _ in range(len(column_stats.index))))})"
        elif dialect == "SQLSERVER":
            cursor.fast_executemany = True
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('?' for _ in range(len(column_stats.index))))})"
        elif dialect == "SQLITE":
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('?' for _ in range(len(column_stats.index))))})"

        try:
            cursor.execute(create_sql)
        except Exception as ex:
            print(ex)
            try:
                cursor.execute(f"DROP TABLE {cleaned_table_name}")
                cursor.execute(create_sql)
            except Exception as ex2:
                print(ex2)
                return -5
        records_inserted = 0
        for i, df in enumerate(data):
            start = time.time()
            end = time.time()
            print(f"Chunk {i + 1}, Data prep: Time elapsed = {end - start} seconds")
            start = time.time()
            rows = (row[1].to_list() for row in df.iterrows())
            if dialect == "POSTGRESQL":
                extras.execute_values(cursor, insert_sql, rows)
            elif dialect in ["ORACLE", "MYSQL", "SQLSERVER", "SQLITE"]:
                cursor.executemany(insert_sql, rows)
            end = time.time()
            records_inserted += len(df.index)
            print(
                f"Chunk {i + 1}, "
                f"Loaded {records_inserted}: Time elapsed = {end - start} seconds"
            )
        connection.commit()
        return records_inserted
