from pandas import DataFrame, merge
from typing import Optional, Callable, Any
from util import (utf8_convert, AnalyzeResult, len_b_str, clean_column_name, dialect_to_col_type,
                  get_db_connection, clean_table_name, columns_needed, LoadResult,
                  check_conflicting_column_info)
import time
from psycopg2 import extras


class DataLoader:

    def __init__(self,
                 data: DataFrame,
                 ini_path: str,
                 db_dialect: str,
                 table_exists: str = "error",
                 value_convert: Optional[Callable[[Any], str]] = None):
        """
        Creates a DataLoader object to assist in the insertion of a DataFrame into a DB

        Parameters
        ----------
        ini_path : str
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
        self.ini_path = ini_path
        self.db_dialect = db_dialect
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
        dialect = self.db_dialect.upper()
        connection = get_db_connection(self.ini_path, self.db_dialect)
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
                if dialect == "SQLITE":
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
        if dialect == "POSTGRESQL":
            extras.execute_values(cursor, insert_sql, rows)
        elif dialect in ["ORACLE", "MYSQL", "SQLSERVER", "SQLITE"]:
            cursor.executemany(insert_sql, rows)
        end = time.time()
        records_inserted += len(self.data.index)
        print(f"Loaded {records_inserted}: Time elapsed = {end - start} seconds")
        connection.commit()
        return LoadResult(1, "", records_inserted)
