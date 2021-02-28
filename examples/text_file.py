from loaders import FileLoader

fa = FileLoader(
    "tblAllTanks.txt",
    "FLAT",
    ini_path="sample_db_config.ini",
    db_dialect="sqlserver",
    table_exists="drop",
    separator=",",
    qualifier=True,
    encoding="utf8"
)
result = fa.analyze_file()
print("Result")
print("--------")
print(f"Code: {result.code}")
print(f"Message: {result.message}")
print(f"Number of records: {result.num_records}")
print(f"Column Stats:\n {result.column_stats}")
