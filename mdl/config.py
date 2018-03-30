# TODO: add bucket_name
NEM12_INPUT_BUCKET = ''
NEM12_PROCESSING_BUCKET = ''
NEM12_DONE_BUCKET = ''
NEM12_PROD_BUCKET = ''

SPMDF_INPUT_BUCKET = ''
SPMDF_PROCESSING_BUCKET = ''
SPMDF_DONE_BUCKET = ''
SPMDF_PROD_BUCKET = ''


# for move_only handler
MOVE_INPUT_BUCKET = ''
MOVE_ARCHIVE_BUCKET = ''

# for unzip
UNZIP_INPUT_BUCKET = ''
UNZIP_PROCESSING_BUCKET = ''



# IMD_15MIN_HEADER_STR = """
#     MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
#     Imp_KVARH	KW 	KVA	source_file_id	MDPUpdateDateTime	QualityCode
#     UOM
#     T_Start
#     T_End
#     Channel
# """
# IMD_30MIN_HEADER_STR = """
#     MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
#     Imp_KVARH	KW	KVA	KVA15	KW15	source_file_id	MDPUpdateDateTime	QualityCode	QualityNumber
#     UOM
#     T_Start
#     T_End
#     Channel
# """
IMD_15MIN_HEADER_STR = """
    MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
    Imp_KVARH	KW 	KVA	source_file_id	QualityCode
    UOM
    T_Start
    T_End
    Channel
"""

IMD_30MIN_HEADER_STR = """
    MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
    Imp_KVARH	KW	KVA	KVA15	KW15	source_file_id	QualityCode
    UOM
    T_Start
    T_End
    Channel
"""

IMD_HEADER_STR = """
    MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
    Imp_KVARH	KW 	KVA	source_file_id	QualityCode
    UOM
    T_Start
    T_End
    Channel
    IntervalLength
"""

IMD_15MIN_HEADER = IMD_15MIN_HEADER_STR.split()
IMD_30MIN_HEADER = IMD_30MIN_HEADER_STR.split()
IMD_HEADER = IMD_HEADER_STR.split()

PERIOD_15MIN = 96
PERIOD_30MIN = 48


UOM = {
    "MWH"	:	1000,
    "KWH"	:	1,
    "WH"	:	0.001,
    "MVARH"	:	1000,
    "KVARH"	:	1,
    "VARH"	:	0.001,
    "MVAR"	:	1000,
    "KVAR"	:	1,
    "VAR"	:	0.001,
    "MW"	:	1000,
    "KW"	:	1,
    "W"	    :	0.001,
    "MVAH"	:	1000,
    "KVAH"	:	1,
    "VAH"	:	0.001,
    "MVA"	:	1000,
    "KVA"	:	1,
    "VA"	:	0.001,
    "KV"	:	1,
    "V"	    :	0.001,
    "KA"	:	1,
    "A"	    :	0.001,
    "PF"	:	1
}
