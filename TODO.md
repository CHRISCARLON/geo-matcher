# TODO

- Missing piece is a row_group_size parameter on GeoParquetWriter in the rust geoarrow io:
- GeoParquetWriter lacks row_group_size — parquet-rs buffers internally so all rows collapse into 1-2 row groups, killing spatial pruning. 
- Once that parameter is exposed upstream, the manual bbox/metadata patching that I have to do can be dropped entirely?
- Make this library generic so that it can take in any dataset you want to spatially match usrns to.
- Do some simple benchmarks
