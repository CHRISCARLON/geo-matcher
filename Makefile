# usrn-matcher — task runner
# Usage: make <target>

.PHONY: \
	init \
	prepare-usrns \
	prepare-soil prepare-stops prepare-counts prepare-built prepare-gas-pipe prepare-all \
	match-soil-national match-soil-national-explain \
	match-soil-leeds match-soil-leeds-explain \
	match-stops-national match-counts-national match-all-national \
	match-gas-pipe-sample match-gas-pipe-national \
	dtf-export-soil-national dtf-export-stops-national dtf-export-counts-national dtf-export-all-national \
	clean-output clean-matched

# ── Setup ─────────────────────────────────────────────────────────────────────

init:
	usrn-matcher init

# ── Prepare USRNs ─────────────────────────────────────────────────────────────

prepare-usrns:
	usrn-matcher prepare-usrns --force

# ── Prepare ───────────────────────────────────────────────────────────────────

prepare-soil:
	usrn-matcher prepare-gpkg \
		--rhs-name soil \
		--force

prepare-built:
	usrn-matcher prepare-gpkg \
		--rhs-name os_open_built_up_areas \
		--force

prepare-stops:
	usrn-matcher prepare-csv \
		--name   stops \
		--x-col  Easting \
		--y-col  Northing \
		--force

prepare-counts:
	usrn-matcher prepare-csv \
		--name   count_points \
		--x-col  easting \
		--y-col  northing \
		--force

prepare-gas-pipe:
	usrn-matcher prepare-parquet \
		--name         gas_pipe \
		--parquet      /Users/cmcarlon/Downloads/gas-pipe-infrastructure-gpi_open.parquet \
		--geometry-col geo_shape \
		--source-crs   EPSG:4326 \
		--threads      2 \
		--force

prepare-all: prepare-soil prepare-stops prepare-counts prepare-built

# ── Match — National (no bbox) ────────────────────────────────────────────────

match-soil-national:
	usrn-matcher match \
		--rhs-name   soil \
		--mode       intersect \
		--output     parquet \

match-stops-national:
	usrn-matcher match \
		--rhs-name stops \
		--mode     nearest \
		--distance 10 \
		--output   parquet \
		--explain

match-counts-national:
	usrn-matcher match \
		--rhs-name count_points \
		--mode     nearest \
		--distance 10 \
		--output   csv \

match-soil-leeds:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     LEEDS \
		--output   csv

match-soil-leeds-explain:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     LEEDS \
		--output   csv \
		--explain

match-soil-national-explain:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--output   csv \
		--explain

match-gas-pipe-sample:
	usrn-matcher match \
		--rhs-name    gas_pipe \
		--mode        line \
		--distance    2 \
		--city        MANCHESTER \
		--output      csv \
		--explain

match-gas-pipe-national:
	usrn-matcher match \
		--rhs-name   gas_pipe \
		--mode       line \
		--distance   2 \
		--rhs-id-col asset_id \
		--threads    4 \
		--output     parquet \
		--explain

match-all-national: match-soil-national match-stops-national match-counts-national

# ── DTF Export — national ─────────────────────────────────────────────────────

dtf-export-soil-national:
	usrn-matcher dtf-export \
		--rhs-name soil \
		--mode     intersect

dtf-export-stops-national:
	usrn-matcher dtf-export \
		--rhs-name stops \
		--mode     nearest

dtf-export-counts-national:
	usrn-matcher dtf-export \
		--rhs-name count_points \
		--mode     nearest

dtf-export-all-national: dtf-export-soil-national dtf-export-stops-national dtf-export-counts-national

# ── Clean ─────────────────────────────────────────────────────────────────────

clean-output:
	rm -f output_data/*.parquet

clean-matched:
	rm -f matched_data/*
