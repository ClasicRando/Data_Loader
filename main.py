from file_loader import FileLoader
import time


start = time.time()
# fa = FileLoader("sample files/bsadb.xlsx", "XLSX", sheet_name="BSA 12 2020")
# fa = FileLoader(
#     r"sample files/Tanks.accdb",
#     "ACCDB",
#     table_name="tbAllRegUSTs"
# )
# fa = FileLoader("sample files/tblAllTanks.txt", "FLAT", separator=",", qualifier=True, encoding="utf8")
# fa = FileLoader("TCEQ_BROWNFIELDS_POINTS.dbf", "DBF")
# result = fa.analyze_file()
# if result.code == 1:
#     print(result.num_records)
#     print(result.column_stats)
# else:
#     print(result.code)
# print(fa.load_file("db_config.ini", "postgresql", "TANK_NC_TBLALLTANKS"))
end = time.time()
print(f"Time elapsed = {end - start} seconds")
