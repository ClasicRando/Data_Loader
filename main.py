from file_loader import FileLoader
import time

# fa = FileLoader("bsadb.xlsx", "XLSX", sheet_name="BSA 12 2020")
fa = FileLoader(
    r"C:\Users\steve\IdeaProjects\Data Analyzer Python\Tanks.accdb",
    "ACCDB",
    table_name="tbAllRegUSTs"
)
start = time.time()
# fa = FileLoader("tblAllTanks.txt", "FLAT", separator=",", qualifier=True, encoding="utf8")
# fa = FileLoader("TCEQ_BROWNFIELDS_POINTS.dbf", "DBF")
result = fa.analyze_file("postgresql")
if result.code == 1:
    print(result.num_records)
    print(result.column_stats)
else:
    print(result.code)
# print(fa.load_file("db_config.ini", "postgresql", "TANK_NC_TBLALLTANKS"))
end = time.time()
print(f"Time elapsed = {end - start} seconds")
