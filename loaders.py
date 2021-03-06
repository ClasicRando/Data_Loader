from pandas import DataFrame, read_excel, read_sql, read_csv, Series, merge
from typing import Generator, Callable, Any, Optional, Union
from sqlalchemy import create_engine
from util import (clean_column_name, clean_table_name, find_encoding, len_b_str, get_db_connection,
                  dialect_to_col_type, convert_column, AnalyzeResult, columns_needed, read_dbf,
                  LoadResult, check_conflicting_column_info, utf8_convert, DbDialect, FileType)
from functools import partial
from psycopg2 import extras
import time
from os.path import abspath
import urllib


class FileLoader:

    def __init__(self,
                 file_path: str,
                 file_type: Union[str, FileType],
                 json_path: str,
                 db_dialect: Union[str, DbDialect],
                 table_exists: str = "error",
                 value_convert: Optional[Callable[[Any], str]] = None,
                 **kwargs):
        """
        Creates a FileLoader object to assist in the insertion of a data file into a DB

        Parameters
        ----------
        file_path : str
            Path to a data file as a string
        file_type : Union[str, FileType]
            Category of data file passed. Currently supported types are: 1) FLAT (.txt,
            .csv, .tsv, etc.) 2) ACCDB (.accdb and .mdb) 3) DBF (.dbf) 4) XLSX (.xlsx)
        json_path : str
            Path to the json file contains the database credentials
        db_dialect : Union[str, DbDialect]
            Database dialect that is to be used for loading the data. Dictates the column types
            If the value is a string, it will try to get the DbDialect Enum. Can raise an exception
            NOTE: This value has to match the json key used.
            Only dialects currently supported are (with the expected json key name after):
                1) Postgresql - postgresql
                2) Oracle - oracle
                3) MySQL - mysql
                4) SQL Server - sqlserver
                5) SQLite - sqlite
        table_exists : str (Default 'error')
            Action to perform when trying to load the data but the already table exists. Can be:
            'drop' - drop the table if it exists
            'append' - add the data. Will raise an error if the columns do not match exactly
            'truncate' - truncate the table's records. Will raise an error if new data does not
                match the columns exactly. This is not supported for SQLite databases
            'error' - if the table already exists return the LoadResult with an error message
        value_convert : (Optional)
            if the user wants to specify how values are to be converted to string objects to be
            inserted into the DB, they can do so here. The expected format is '(Any) -> str' as to
            account for Pandas inferring data types but always converting the DataFrame elements to
            string objects

        Keyword arguments
        -----------------
        separator : str
            delimited data's separator character. Defaults to ','
        qualifier : bool
            whether or not the data is qualified. Only accept data with '"' as the qualifier.
            Defaults to False
        encoding : str
            encoding of the text file or DBF. Defaults to 'utf8' and falls back on 'cp1252' if not
            'utf8'. To improve performance (since the whole file needs to be read to infer encoding)
            please provide this value if you know it and want to load a text file or DBF
        table_name : str
            name of the table to extract for an access database. Required for ACCDB
        sheet_name : str
            name of the sheet to extract for the excel file. Required for XLSX
        chunk_size : int
            Number of records to read into memory at a given time. Does not affect operations of
            Excel files since chunking is not supported
        """
        self.path = abspath(file_path)
        self.file_type = FileType(file_type) if isinstance(file_type, str) else file_type
        self.json_path = json_path
        self.db_dialect = DbDialect(db_dialect) if isinstance(db_dialect, str) else db_dialect
        self.table_exits = table_exists
        if self.table_exits not in ["append", "drop", "truncate", "error"]:
            raise Exception(
                "Table_exists parameter does not match the intended input."
                "Please consult docs to provide the right action or don't pass the parameter"
            )
        self.encoding = "utf8"
        self.chunk_size = kwargs["chunk_size"] if "chunk_size" in kwargs else 10000
        if self.file_type == FileType.FLAT:
            self.separator = kwargs["separator"] if "separator" in kwargs else ","
            self.qualifier = kwargs["qualifier"] if "qualifier" in kwargs else False
            if "encoding" in kwargs:
                self.encoding = kwargs["encoding"]
            else:
                self.encoding = find_encoding(self.path, self.file_type)
        elif self.file_type == FileType.ACCDB:
            if "table_name" in kwargs:
                self.table_name = kwargs["table_name"]
            else:
                raise Exception(
                    "table_name in access file was not passed as keyword argument."
                    "Please pass a table name from the accdb to continue"
                )
        elif self.file_type == FileType.DBF:
            if "encoding" in kwargs:
                self.encoding = kwargs["encoding"]
            else:
                self.encoding = find_encoding(self.path, self.file_type)
        elif self.file_type == FileType.XLSX:
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
        if self.file_type == FileType.FLAT:
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
        elif self.file_type == FileType.ACCDB:
            conn_str = "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + self.path
            conn_uri = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
            accdb_engine = create_engine(conn_uri)
            data = read_sql(
                f"select * from {self.table_name}",
                accdb_engine,
                chunksize=self.chunk_size
            )
        elif self.file_type == FileType.DBF:
            data = read_dbf(
                self.path,
                self.encoding,
                self.chunk_size
            )
        elif self.file_type == FileType.XLSX:
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

    def analyze_file(self) -> AnalyzeResult:
        """
        Method to analyze the given file/data and produce column stats as well as loading parameters

        Returns
        -------
        Dataclass that holds the result code, number of records and column stats (as a DataFrame)
        """
        b_max_len: Series = Series()
        b_min_len: Series = Series()
        num_records = 0
        try:
            for i, df in enumerate(self.get_data()):
                num_records += len(df.index)
                b_lens = df.applymap(len_b_str)
                if b_max_len.empty:
                    b_max_len = b_lens.max()
                    b_min_len = b_lens.min()
                else:
                    b_max_len = b_max_len.combine(b_lens.max(), max)
                    b_min_len = b_max_len.combine(b_lens.min(), min)
                print(f"Done analyzing Chunk {i + 1}")
        except Exception as ex:
            return AnalyzeResult(-1, f"Error {ex} while analyzing file")
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
        df["Column Type"] = df["Max Len"].map(dialect_to_col_type[self.db_dialect.upper()])
        return AnalyzeResult(1, "", num_records, df)

    def load_file(
            self,
            table_name: str,
            column_stats: Optional[DataFrame] = None) -> LoadResult:
        """
        Method to load the referenced file/data into the database specified by the ini file

        Parameters
        ----------
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
        connection = get_db_connection(self.json_path, self.db_dialect)
        cursor = connection.cursor()
        cleaned_table_name = clean_table_name(table_name)
        if column_stats is None:
            print("Getting Column Stats")
            result = self.analyze_file()
            if result.code != 1:
                return LoadResult(-1, f"Error while analyzing file. {result.message}")
            column_stats = result.column_stats
            print("Done getting Column Stats")
        if any([c not in column_stats.columns for c in columns_needed]):
            return LoadResult(
                -2,
                "Column stats provided or generated did not have the columns/data needed to load "
                "the file "
            )
        try:
            data = self.get_data()
        except Exception as ex:
            return LoadResult(-3, f"Error trying to generate the DataFrames {ex}")
        if data is None:
            return LoadResult(-4, "Data generator was None. What?!?!")
        data_types = list(map(
            lambda x: f"{x[0]} {x[1]}",
            column_stats.set_index("Column Name Formatted")["Column Type"].items()
        ))
        columns_names = ",".join(column_stats["Column Name Formatted"].to_list())
        create_sql = f"CREATE TABLE {cleaned_table_name}({','.join(data_types)})"

        insert_sql = ""
        if self.db_dialect == DbDialect.POSTGRESQL:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES %s"
        elif self.db_dialect == DbDialect.ORACLE:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join((f':{i + 1}' for i in range(len(column_stats.index))))})"
        elif self.db_dialect == DbDialect.MYSQL:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('%s' for _ in range(len(column_stats.index))))})"
        elif self.db_dialect == DbDialect.SQLSERVER:
            cursor.fast_executemany = True
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('?' for _ in range(len(column_stats.index))))})"
        elif self.db_dialect == DbDialect.SQLITE:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('?' for _ in range(len(column_stats.index))))})"

        try:
            cursor.execute(create_sql)
        except Exception as ex:
            if self.table_exits == "error":
                return LoadResult(
                    -5,
                    f"Table already exists and table_exists was set to error {ex}"
                )
            elif self.table_exits == "drop":
                try:
                    cursor.execute(f"DROP TABLE {cleaned_table_name}")
                    cursor.execute(create_sql)
                except Exception as ex2:
                    return LoadResult(-5, f"Error trying to drop table. {ex2}")
            elif self.table_exits == "append":
                if check_conflicting_column_info(cursor, self.db_dialect, table_name, column_stats):
                    return LoadResult(
                        -5,
                        f"Error trying to append to existing table. "
                        f"Columns cannot be different in name or type."
                    )
            elif self.table_exits == "truncate":
                if check_conflicting_column_info(cursor, self.db_dialect, table_name, column_stats):
                    return LoadResult(
                        -5,
                        f"Error trying to truncate existing table. "
                        f"Columns cannot be different in name or type."
                    )
                if self.db_dialect == DbDialect.SQLITE:
                    return LoadResult(-5, "Truncate not supported by SQLITE")
                else:
                    command_sql = f"TRUNCATE TABLE {cleaned_table_name}"
                try:
                    cursor.execute(command_sql)
                except Exception as ex2:
                    return LoadResult(-5, f"Error trying to truncate table. {ex2}")

        records_inserted = 0
        for i, df in enumerate(data):
            start = time.time()
            rows = (row[1].to_list() for row in df.iterrows())
            if self.db_dialect == DbDialect.POSTGRESQL:
                extras.execute_values(cursor, insert_sql, rows)
            else:
                cursor.executemany(insert_sql, rows)
            end = time.time()
            records_inserted += len(df.index)
            print(
                f"Chunk {i + 1}, "
                f"Loaded {records_inserted}: Time elapsed = {end - start} seconds"
            )
        connection.commit()
        return LoadResult(1, "", records_inserted)


class DataLoader:

    def __init__(self,
                 data: DataFrame,
                 json_path: str,
                 db_dialect: Union[str, DbDialect],
                 table_exists: str = "error",
                 value_convert: Optional[Callable[[Any], str]] = None):
        """
        Creates a DataLoader object to assist in the insertion of a DataFrame into a DB

        Parameters
        ----------
        json_path : str
            Path to the ini file contains the database credentials
        db_dialect : str
            Database dialect that is to be used for loading the data. Dictates the column types
            NOTE: This value has to match the ini section header used.
            Only dialects currently supported are (with the expected ini section name after):
                1) Postgresql - postgresql
                2) Oracle - oracle
                3) MySQL - mysql
                4) SQL Server - sqlserver
                5) SQLite - sqlite
        table_exists : str (Default 'error')
            Action to perform when trying to load the data but the already table exists. Can be:
            'drop' - drop the table if it exists
            'append' - add the data. Will raise an error if the columns do not match exactly
            'truncate' - truncate the table's records. Will raise an error if new data does not
                match the columns exactly. This is not supported for SQLite databases
            'error' - if the table already exists return the LoadResult with an error message
        value_convert : (Optional) if the user wants to specify how values are to be converted
            to string objects to be inserted into the DB, they can do so here. The expected format
            is '(Any) -> str' as to account for Pandas inferring data types but always converting
            the DataFrame elements to string objects
        """
        self.data = data
        self.json_path = json_path
        self.db_dialect = DbDialect(db_dialect) if isinstance(db_dialect, str) else db_dialect
        self.table_exits = table_exists
        self.value_convert = value_convert if value_convert is not None else utf8_convert

    def analyze_data(self) -> AnalyzeResult:
        """
        Method to analyze the given data and produce column stats as well as loading parameters

        Returns
        -------
        Dataclass that holds the result code, number of records and column stats (as a DataFrame)
        """
        num_records = 0
        try:
            num_records += len(self.data.index)
            b_lens = self.data.applymap(len_b_str)
            b_max_len = b_lens.max()
            b_min_len = b_lens.min()
        except Exception as ex:
            return AnalyzeResult(-1, f"Error {ex} while analyzing DataFrame")
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
        df["Column Type"] = df["Max Len"].map(dialect_to_col_type[self.db_dialect.upper()])
        return AnalyzeResult(1, "", num_records, df)

    def load_data(
            self,
            table_name: str,
            column_stats: Optional[DataFrame] = None) -> LoadResult:
        """
        Method to load the referenced data into the database specified by the ini file

        Parameters
        ----------
        table_name : str
            name used when inserting the records into the database
        column_stats : Optional[DataFrame]
            if you have already analyzed the data and have the DataFrame needed to provided column
            stats, you can provide this optional parameter. If nothing is provided then the
            'analyze_data' function is called to get the column stats. If a DataFrame is provided,
            the columns will be checked to ensure it meets the needs to provide the create table
            statement and an Exception will be raised if it does not pass the test

        Returns
        -------
        Number of records inserted. A negative result means an error occurred
        """
        connection = get_db_connection(self.json_path, self.db_dialect)
        cursor = connection.cursor()
        cleaned_table_name = clean_table_name(table_name)
        if column_stats is None:
            print("Getting Column Stats")
            result = self.analyze_data()
            if result.code != 1:
                return LoadResult(-1, f"Error while analyzing file. {result.message}")
            column_stats = result.column_stats
            print("Done getting Column Stats")
        if any([c not in column_stats.columns for c in columns_needed]):
            return LoadResult(
                -2,
                "Column stats provided or generated did not have the columns/data needed to load "
                "the file "
            )
        data_types = list(map(
            lambda x: f"{x[0]} {x[1]}",
            column_stats.set_index("Column Name Formatted")["Column Type"].items()
        ))
        columns_names = ",".join(column_stats["Column Name Formatted"].to_list())
        create_sql = f"CREATE TABLE {cleaned_table_name}({','.join(data_types)})"

        insert_sql = ""
        if self.db_dialect == DbDialect.POSTGRESQL:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES %s"
        elif self.db_dialect == DbDialect.ORACLE:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join((f':{i + 1}' for i in range(len(column_stats.index))))})"
        elif self.db_dialect == DbDialect.MYSQL:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('%s' for _ in range(len(column_stats.index))))})"
        elif self.db_dialect == DbDialect.SQLSERVER:
            cursor.fast_executemany = True
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('?' for _ in range(len(column_stats.index))))})"
        elif self.db_dialect == DbDialect.SQLITE:
            insert_sql = f"INSERT INTO {cleaned_table_name}({columns_names}) VALUES " \
                         f"({','.join(('?' for _ in range(len(column_stats.index))))})"

        try:
            cursor.execute(create_sql)
        except Exception as ex:
            if self.table_exits == "error":
                return LoadResult(
                    -5,
                    f"Table already exists and table_exists was set to error {ex}"
                )
            elif self.table_exits == "drop":
                try:
                    cursor.execute(f"DROP TABLE {cleaned_table_name}")
                    cursor.execute(create_sql)
                except Exception as ex2:
                    return LoadResult(-5, f"Error trying to drop table. {ex2}")
            elif self.table_exits == "append":
                if check_conflicting_column_info(cursor, self.db_dialect, table_name, column_stats):
                    return LoadResult(
                        -5,
                        f"Error trying to append to existing table. "
                        f"Columns cannot be different in name or type."
                    )
            elif self.table_exits == "truncate":
                if check_conflicting_column_info(cursor, self.db_dialect, table_name, column_stats):
                    return LoadResult(
                        -5,
                        f"Error trying to truncate existing table. "
                        f"Columns cannot be different in name or type."
                    )
                if self.db_dialect == DbDialect.SQLITE:
                    return LoadResult(-5, "Truncate not supported by SQLITE")
                else:
                    command_sql = f"TRUNCATE TABLE {cleaned_table_name}"
                try:
                    cursor.execute(command_sql)
                except Exception as ex2:
                    return LoadResult(-5, f"Error trying to truncate table. {ex2}")

        records_inserted = 0
        start = time.time()
        rows = (row[1].to_list() for row in self.data.iterrows())
        if self.db_dialect == DbDialect.POSTGRESQL:
            extras.execute_values(cursor, insert_sql, rows)
        else:
            cursor.executemany(insert_sql, rows)
        end = time.time()
        records_inserted += len(self.data.index)
        print(f"Loaded {records_inserted}: Time elapsed = {end - start} seconds")
        connection.commit()
        return LoadResult(1, "", records_inserted)
