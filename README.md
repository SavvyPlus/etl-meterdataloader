# Meter Loader

## Purpose
The MeterDataLoader application is a tool to process interval meter data files and load into the MeterDataDB database. The application supports industry-standard NEM12 and a fully flexible range of CSV formats. It is structured to allow the addition of extra handlers for other file types if required.

As at May 2017 the main 15- and 30-minute meter data tables contain over 200m records representing meter data for around 2100 meters.

## Handlers
There are four separate handlers in MeterLoader

* `meter-loader-nem` creates IMD_15 and IMD_30 tables for NEM files
* `meter-loader-nem-others` creates NEMMDF_B2BDetailes,
NEMMDF_FileDetails,
NEMMDF_IntervalData,
NEMMDF_QualityDetails,
NEMMDF_Stream,
NEMMDF_StreamDay and
NEMMDF_StreamDetails tables
* `meter-loader-spmdf` creates IMD_15 and IMD_30 tables for custom files (spmdf files)
* `meter-loader-spmdf-other` create SPMDF_MeterDetails table

*Notes `NEMMDF_UOM` is static table. We need to load data of this table manually to Athena. `NEMMDF_UOM` data can be found at `./athena/static_tables/NEMMDF_UOM.csv`*

### meter-loader-nem
This handler only needs one input folder

### meter-loader-others
This handler only needs one input folder

### meter-loader-spmdf
This handler needs different input folder for each retailer

### meter-loader-spmdf-other
This handler needs different input folder for each retailer


## SPMDF config
`meter-loader-spmdf/spmdf_config.py`
and
`meter-loader-spmdf-other/spmdf_config.py` are indentical.

```
"TasWaterrawdata*.csv": {
  "source": "TasWater",
  "pattern": "rawdata*.csv",
  "params": {'usecols':['NMI','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
  'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,'fixed_column_vals': {'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},'map_col_names':{'NMI':'MeterRef',
  'REPORT_DATE':'Date', 'REPORT_TIME':'Time', 'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH',
  'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'}}
},
```

* **source**: retailer name
* **pattern**: file name pattern to match the config
* **params**: this value is passed to Pandas function. This value is defined in MeterDataLoaderJobs table in MeterData DB.
* **key of config**: key to identify the config is a combination of retailer name and file pattern
