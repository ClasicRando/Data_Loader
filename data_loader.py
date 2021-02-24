from pandas import DataFrame, merge
from typing import Optional, Callable, Any
from util import (utf8_convert, AnalyzeResult, len_b_str, clean_column_name, dialect_to_col_type,
                  get_db_connection, clean_table_name, columns_needed)
import time
from psycopg2 import extras


class DataLoader:

    def __init__(self, data: DataFrame, value_convert: Optional[Callable[[Any], str]] = None):
        """
        Creates a DataLoader object to assist in the insertion of a DataFrame into a DB

        Parameters
        ----------
        value_convert : (Optional) if the user wants to specify how values are to be converted
            to string objects to be inserted into the DB, they can do so here. The expected format
            is '(Any) -> str' as to account for Pandas inferring data types but always converting
            the DataFrame elements to string objects
        """
        self.data = data
        self.value_convert = value_convert if value_convert is not None else utf8_convert

    def analyze_data(self, dialect: str) -> AnalyzeResult:
        """
        Method to analyze the given data and produce column stats as well as loading parameters

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
        num_records = 0
        try:
            num_records += len(self.data.index)
            b_lens = self.data.applymap(len_b_str)
            b_max_len = b_lens.max()
            b_min_len = b_lens.min()
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

    def load_data(
            self,
            ini_path: str,
            ini_section: str,
            table_name: str,
            column_stats: Optional[DataFrame] = None) -> int:
        """
        Method to load the referenced data into the database specified by the ini file

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
            if you have already analyzed the data and have the DataFrame needed to provided column
            stats, you can provide this optional parameter. If nothing is provided then the
            'analyze_data' function is called to get the column stats. If a DataFrame is provided,
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
            result = self.analyze_data(dialect)
            if result.code != 1:
                return -1
            column_stats = result.column_stats
            print("Done getting Column Stats")
        if any([c not in column_stats.columns for c in columns_needed]):
            return -3
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
        start = time.time()
        end = time.time()
        print(f"Data prep: Time elapsed = {end - start} seconds")
        start = time.time()
        rows = (row[1].to_list() for row in self.data.iterrows())
        if dialect == "POSTGRESQL":
            extras.execute_values(cursor, insert_sql, rows)
        elif dialect in ["ORACLE", "MYSQL", "SQLSERVER", "SQLITE"]:
            cursor.executemany(insert_sql, rows)
        end = time.time()
        records_inserted += len(self.data.index)
        print(f"Loaded {records_inserted}: Time elapsed = {end - start} seconds")
        connection.commit()
        return records_inserted
