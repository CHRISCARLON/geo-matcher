import pathlib

_TMP = pathlib.Path("output_data/_tmp_convert.parquet")

SOIL_GPKG = pathlib.Path("input_data/NationalSoilMap.gpkg")
USRN_GPKG = pathlib.Path("input_data/osopenusrn.gpkg")

SOIL_PARQUET = pathlib.Path("output_data/soil_27700.parquet")
USRN_PARQUET = pathlib.Path("output_data/usrns_27700.parquet")
