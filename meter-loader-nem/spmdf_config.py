SPMDF_CONFIG = {
    "BOCrawdata*.csv" : {
        "source": "BOC",
        "pattern": "rawdata*.csv",
        "params": {'usecols':['NMI','METER_POINT','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
                    'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,
                    'fixed_column_vals':{'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},
                    'map_col_names':{'REPORT_DATE':'Date', 'REPORT_TIME':'Time', 'METER_POINT':'StreamRef',
                    'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH', 'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'},
                    'flip_signs':['Imp_KWH']}
    },
    "TasDoErawdata*.csv": {
        "source": "TasDoE",
        "pattern": "rawdata*.csv",
        "params": {'usecols':['NMI','METER_POINT','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
                   'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,'fixed_column_vals':
                   {'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},'map_col_names':{'REPORT_DATE':'Date',
                   'REPORT_TIME':'Time', 'METER_POINT':'StreamRef', 'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH',
                   'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'}, 'flip_signs':['Imp_KWH']}
    },
    "TasWaterMeter*.csv": {
        "source": "TasWater",
        "pattern": "Meter*.csv",
        "params": {'usecols':['NMI','ReadingDateTime','Quality','E (Usage kWh)','B (Generation kWh)','K (Generation kvarh)',
                   'Q (Usage kvarh)'],'parse_dates':['ReadingDateTime'],'dayfirst':True,'header':0,'fixed_column_vals':
                   {'IntervalLength':15,'TimestampType':'PS'},'map_col_names':{'NMI':'MeterRef','Quality':'QualityCode',
                   'ReadingDateTime':'Timestamp', 'E (Usage kWh)':'Exp_KWH', 'B (Generation kWh)':'Imp_KWH',
                   'Q (Usage kvarh)':'Exp_KVARH', 'K (Generation kvarh)':'Imp_KVARH'}, 'truncateNMI':True,
                   'encoding':'ascii','header_end_text':'\xef\xbb\xbf'}
    },
    "GoannaMeter*.csv": {
        "source": "Goanna",
        "pattern": "Meter*.csv",
        "params": {'usecols':['NMI','ReadingDateTime','Quality','E (Usage kWh)','B (Generation kWh)',
                   'K (Generation kvarh)','Q (Usage kvarh)'],'parse_dates':['ReadingDateTime'],'dayfirst':True,'header':0,
                   'fixed_column_vals':{'IntervalLength':15,'TimestampType':'PS'},'map_col_names':
                   {'NMI':'MeterRef','Quality':'QualityCode','ReadingDateTime':'Timestamp', 'E (Usage kWh)':'Exp_KWH',
                   'B (Generation kWh)':'Imp_KWH', 'Q (Usage kvarh)':'Exp_KVARH', 'K (Generation kvarh)':'Imp_KVARH'},
                   'truncateNMI':True}
    },
    "TasWaterrawdata*.csv": {
        "source": "TasWater",
        "pattern": "rawdata*.csv",
        "params": {'usecols':['NMI','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
                   'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,'fixed_column_vals':
                   {'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},'map_col_names':{'NMI':'MeterRef',
                   'REPORT_DATE':'Date', 'REPORT_TIME':'Time', 'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH',
                   'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'}}
    },
    "TasWaterRawData*.csv": {
        "source": "TasWater",
        "pattern": "RawData*.csv",
        "params": {'usecols':['NMI','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
                   'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,'fixed_column_vals':
                   {'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},'map_col_names':{'NMI':'MeterRef',
                   'REPORT_DATE':'Date', 'REPORT_TIME':'Time', 'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH',
                   'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'}}
    },
    "TasDoEDOErawdata*.csv": {
        "source": "TasDoE",
        "pattern": "DOE rawdata*.csv",
        "params": {'usecols':['NMI','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
                   'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,'fixed_column_vals':
                   {'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},'map_col_names':
                   {'NMI':'MeterRef', 'REPORT_DATE':'Date', 'REPORT_TIME':'Time', 'KVARH_CON':'Exp_KVARH',
                   'KVARH_GEN':'Imp_KVARH', 'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'}}
    },
    "Ad-hocSP*.csv": {
        "source": "Ad-hoc",
        "pattern": "SP*.csv",
        "params": {'parse_dates':['Timestamp'],'dayfirst':True}
    },
    "TasDoErawdata*.csv": {
        "source": "TasDoE",
        "pattern": "rawdata*.csv",
        "params": {'usecols':['NMI','METER_POINT','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
                   'parse_dates':['REPORT_DATE'],'dayfirst':True,'header':0,'fixed_column_vals':
                   {'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'},'map_col_names':{'REPORT_DATE':'Date',
                   'REPORT_TIME':'Time', 'METER_POINT':'StreamRef', 'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH',
                   'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'}}
    },
    "Ad-hocMeter*.csv": {
        "source": "Ad-hoc",
        "pattern": "Meter*.csv",
        "params": {'usecols':['NMI','ReadingDateTime','Quality','E (Usage kWh)','B (Generation kWh)',
                   'K (Generation kvarh)','Q (Usage kvarh)'],'parse_dates':['ReadingDateTime'],
                   'dayfirst':True,'header':0,'fixed_column_vals':{'IntervalLength':15,'TimestampType':'PS'},
                   'map_col_names':{'NMI':'MeterRef','Quality':'QualityCode','ReadingDateTime':'Timestamp',
                   'E (Usage kWh)':'Exp_KWH', 'B (Generation kWh)':'Imp_KWH', 'Q (Usage kvarh)':'Exp_KVARH',
                   'K (Generation kvarh)':'Imp_KVARH'}, 'truncateNMI':True,
                   'encoding':'ascii','header_end_text':'\xef\xbb\xbf'}
    }
}

# SPMDF_PARMS = {
#     "BOC": {
#             'usecols':[
#             'NMI','METER_POINT','REPORT_DATE','REPORT_TIME','KVARH_CON','KVARH_GEN','KWH_CON','KWH_GEN'],
#             'parse_dates':['REPORT_DATE'],
#             'dayfirst':True,
#             'header':0,
#             'fixed_column_vals': {
#                 'IntervalLength':15,'TimestampType':'PE','QualityCode':'X'
#             },
#             'map_col_names': {
#                 'REPORT_DATE':'Date', 'REPORT_TIME':'Time', 'METER_POINT':'StreamRef',
#                 'KVARH_CON':'Exp_KVARH', 'KVARH_GEN':'Imp_KVARH', 'KWH_CON':'Exp_KWH', 'KWH_GEN':'Imp_KWH'
#             },
#             'flip_signs':['Imp_KWH']
#         },
#     "DOE": {
#             'usecols':
#                 ['NMI','ReadingDateTime','Quality','E (Usage kWh)','B (Generation kWh)','K (Generation kvarh)','Q (Usage kvarh)'],
#             'parse_dates':['ReadingDateTime'],
#             'dayfirst':True,
#             'header':0,
#             'fixed_column_vals':{'IntervalLength':15,'TimestampType':'PS'},
#             'map_col_names':{
#             'NMI':'MeterRef','Quality':'QualityCode','ReadingDateTime':'Timestamp', 'E (Usage kWh)':'Exp_KWH',
#             'B (Generation kWh)':'Imp_KWH', 'Q (Usage kvarh)':'Exp_KVARH', 'K (Generation kvarh)':'Imp_KVARH'},
#             'truncateNMI':True, 'encoding':'ascii','header_end_text':'\xef\xbb\xbf'
#         },
#     "TAS_WATER": {
#             'usecols': [
#                 'NMI','ReadingDateTime','Quality','E (Usage kWh)','B (Generation kWh)','K (Generation kvarh)',
#                 'Q (Usage kvarh)'
#             ],
#             'parse_dates':['ReadingDateTime'],
#             'dayfirst': True,
#             'header':0,
#             'fixed_column_vals': {
#                 'IntervalLength':15,'TimestampType':'PS'
#             },
#             'map_col_names': {
#                 'NMI':'MeterRef','Quality':'QualityCode','ReadingDateTime':'Timestamp',
#                 'E (Usage kWh)':'Exp_KWH', 'B (Generation kWh)':'Imp_KWH', 'Q (Usage kvarh)':'Exp_KVARH',
#                 'K (Generation kvarh)':'Imp_KVARH'
#             },
#             'truncateNMI': True,
#             'encoding': 'ascii',
#             'header_end_text': '\xef\xbb\xbf'
#         },
#
#     "AD-HOC": {
#     'usecols':
#     ['NMI','ReadingDateTime','Quality','E (Usage kWh)','B (Generation kWh)','K (Generation kvarh)','Q (Usage kvarh)'],
#     'parse_dates':['ReadingDateTime'],
#     'dayfirst':True,'header':0,'fixed_column_vals':{'IntervalLength':15,'TimestampType':'PS'},
#     'map_col_names':{
#     'NMI':'MeterRef','Quality':'QualityCode','ReadingDateTime':'Timestamp', 'E (Usage kWh)':'Exp_KWH',
#     'B (Generation kWh)':'Imp_KWH', 'Q (Usage kvarh)':'Exp_KVARH', 'K (Generation kvarh)':'Imp_KVARH'}, 'truncateNMI':True,
#      'encoding':'ascii','header_end_text':'\xef\xbb\xbf'
#     }
# }
