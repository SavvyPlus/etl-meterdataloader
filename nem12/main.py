import os
import uuid
from datetime import datetime
from decimal import Decimal
from re import match, sub


def throws_a(func, *exceptions):
    try:
        func()
        return False
    except exceptions or Exception:
        return True


# checks that all tokens comply with MDFF requirements on length, data type, manadatory etc.
def mdf_length_type_check(toks, fields, data_types, lengths, mandatory):
    if not len(fields) == len(data_types) == len(lengths) == len(mandatory):
        raise Exception("Invalid configuration passed to mdf_length_type_check. Lengths are %d %d %d %d", len(fields),
                        len(data_types), len(lengths), len(mandatory))
    if not len(fields) == len(toks):
        error_text = 'Invalid token stream in meter data file. Too many or too few tokens. Expecting %d, found %d' %  \
                     len(fields), len(toks[1:])
        print(error_text)
        return (False, [])
    tok_length = list(map(len, toks))
    vals = []

    for i in range(0, len(fields)):  # check each field
        # warn if leading or trailing spaces, but remove and continue
        if toks[i].strip() != toks[i]:
            print('Leading or trailing whitespace found in field %s. Found: "%s"', fields[i], toks[i])
            toks[i] = toks[i].strip()
            tok_length[i] = len(toks[i])

        if mandatory[i] and tok_length[i] == 0:  # for missing mandatory values
            error_text = 'Missing a mandatory value in field %s' % fields[i]
            print(error_text)
            return (False, [])
        if data_types[i] == 'C' and tok_length[i] not in (0, lengths[i]):
            error_text = 'Fixed length string of incorrect length in field %s. Expecting %d characters, found "%s"' % \
                         fields[i], lengths[i], toks[i]
            print(error_text)
            return (False, [])
        if data_types[i] == 'V' and tok_length[i] > lengths[i]:
            error_text = 'Value exceeds maximum allowed length in field %s. Expecting %d characters, found %d. Value is "%s"' % \
                         fields[i], lengths[i], tok_length[i], toks[i]
            print(error_text)
            return (False, [])
        if data_types[i] == 'D' and tok_length[i] not in (0, lengths[i]):
            error_text = 'Datetime value of incorrect length in field %s. Expecting %d characters, found "%s"' % fields[i], lengths[i], toks[i]
            print(error_text)
            return (False, [])
        if data_types[i] == 'D' and tok_length[i] != 0:  # check that toks[i] contains a valid date
            try:
                if lengths[i] > 12:
                    s = int(toks[i][12:14])
                else:
                    s = 0
                if lengths[i] > 8:
                    h = int(toks[i][8:10])
                    m = int(toks[i][10:12])
                else:
                    h = m = 0
                datetime(int(toks[i][0:4]), int(toks[i][4:6]), int(toks[i][6:8]), h, m, s)
            except ValueError:
                error_text = 'Invalid date value. Expecting Datetime(%d), found "%s"' % lengths[i], toks[i]
                print(error_text)
                return (False, [])
        if data_types[i] == 'N' and int(lengths[i]) == lengths[i]:  # expecting integer
            if tok_length[i] > lengths[i]:
                error_text = 'Value exceeds maximum allowed length in field %s. Expecting max of %d characters, found %d. Value is "%s"' % fields[i], lengths[i], tok_length[i], toks[i]
                print(error_text)
                return (False, [])
            if len(toks[i]) > 0 and throws_a(lambda: int(toks[i]), ValueError):
                error_text = 'Invalid value encountered in field %s. Expecting integer, found "%s"', fields[i], toks[i]
                print(error_text)
                return (False, [])
        if data_types[i] == 'N' and int(lengths[i]) != lengths[i]:  # expecting float
            (pre, post) = map(int, str(lengths[i]).split('.'))  # max digits expected before & after decimal place
            s = toks[i].split('.')
            if len(s[0]) > pre or (len(s) > 1 and len(s[1]) > post):
                error_text = 'Value exceeds maximum allowed length or precision in field %s. Expecting Numeric(%d.%d), found "%s"' % fields[i], pre, post, toks[i]
                print(error_text)
                return (False, [])
            if len(s) > 2 or (len(s[0]) > 0 and throws_a(lambda: int(s[0]), ValueError)) or (
                    len(s) == 2 and (throws_a(lambda: int(s[1]), ValueError) or int(s[1]) < 0)) or throws_a(
                    lambda: float(toks[i]), ValueError):
                error_text = 'Invalid value encountered in field %s. Expecting Numeric(%d.%d), found "%s"' % fields[i], pre, post, toks[i]
                print(error_text)
                return False, []

        if data_types[i] in ("C", "V"):
            val = toks[i]
        elif data_types[i] == "D" and len(toks[i]) == 0:  # null date
            # val = None
            val = ''
        elif data_types[i] == "D" and len(toks[i]) > 0:
            val = datetime(int(toks[i][0:4]), int(toks[i][4:6]), int(toks[i][6:8]), h, m, s)
        elif data_types[i] == "N" and int(lengths[i]) != lengths[i] and len(toks[i]) > 0:
            # val = Decimal(toks[i])
            val = toks[i]
        elif data_types[i] == "N" and int(lengths[i]) == lengths[i] and len(toks[i]) > 0:
            # val = int(toks[i])
            val = toks[i]
        elif data_types[i] == "N" and len(toks[i]) == 0:
            # val = None
            val = ''
        else:
            error_text = 'Unhandled data type encountered in field %s' % fields[i]
            print (error_text)
            return False, []

        vals.append(val)

    return True, vals


def createCSV(fname, headers, vals):
    # res variable will cach the merged table
    # ----
    res = [headers]

    for each in vals:
        res.append(each)
    # transform the list to byte stream and return
    list_res = list(map(lambda line: ','.join(line), res))
    str_res = '\n'.join(list_res)
    bytes_res = str_res.encode()

    do_print = True
    if do_print:
        with open(fname + '.csv', 'wb') as w:
            w.write(bytes_res)
    return bytes_res
    # ----!


def process_MDF_100(version_header,toks,last_rec_ind):
    valid_after = [None]
    table_name = 'NEMMDF_Staging_100'
    fields = ['VersionHeader', 'DateTime', 'FromParticipant', 'ToParticipant']
    merge_keys = []
    data_types = 'VDVV'
    lengths = [5, 12, 10, 10]
    mandatory = [True, True, True, True]

    if not last_rec_ind in valid_after:  # check for file blocking errors
        print('MDF-100: Meter data file blocking error')
        return False, []

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return False, []

    # print(vals)
    # specific checks
    if (version_header is not None) and version_header != toks[0]:
        error_text = 'VersionHeader in filename and 100 record do not match. Filename has %s and 100-record has %s' % version_header, \
                     toks[0]
        print(error_text)
        return False, []
    if toks[0] not in ("NEM12", "NEM13"):
        error_text = 'VersionHeader in 100 record is invalid. Requires NEM12 or NEM13, found %s' % toks[0]
        print(error_text)
        return False, []

    # no merge keys so just insert to database
    # sql = sql_mdff_merge_statement(table_name, fields + ['source_file_id'], merge_keys)
    # curs = conn.cursor()
    # curs.execute(sql, tuple(vals) + (source_file_id,))
    # thisid = curs.fetchone()[0]
    # curs.close()

    # prepare CSV data
    fields.append('FileId')
    thisid = str(uuid.uuid4())
    vals.append(thisid)
    vals[1] = vals[1].strftime('%Y-%m-%d %H:%M')
    createCSV('staging100', fields, [vals])

    return True, vals


def process_MDF_200(version_header,toks,last_rec_ind, last100):
    valid_after = ['100', '300', '400', '500']
    table_name = 'NEMMDF_Staging_200'
    fields = ['NMI', 'NMIConfiguration', 'RegisterID', 'NMISuffix', 'MDMDataStreamIdentifier', 'MeterSerialNumber',
              'UOM', 'IntervalLength', 'NextScheduledReadDate']
    merge_keys = []
    data_types = 'CVVCCVVND'
    lengths = [10, 240, 10, 2, 2, 12, 5, 2, 8]
    mandatory = [True, True, False, True, False, False, True, True, False]

    if not last_rec_ind in valid_after:  # check for file blocking errors
        error_text = 'Meter data file blocking error'
        print(error_text)
        return False, []

    # allocate tokens, confirm data types and field lengths as required
    (status, vals) = mdf_length_type_check(toks, fields, data_types, lengths, mandatory)
    if not status:
        return False, []

    # specific checks
    # valid NMIConfiguration
    if not all(map(lambda x: match(r'[A-HJ-NP-Z][1-9A-HJ-NP-Z]', x) is not None,
                   [vals[1][i:i + 2] for i in range(0, len(vals[1]), 2)])):
        error_text = 'Invalid NMI Configuration. Found %s' % vals[1]
        print(error_text)
        return False, []
    # valid RegisterID?
    # valid NMISuffix
    if match(r'[A-HJ-NP-Z][1-9A-HJ-NP-Z]', vals[3]) is None:
        error_text = 'Invalid NMI Suffix. Found %s' % vals[3]
        print(error_text)
        return False, []
    # valid MDMDataStreamIdentifier?
    # valid UOM
    if not vals[6].lower() in (
    'mwh', 'kwh', 'wh', 'mvarh', 'kvarh', 'varh', 'mvar', 'kvar', 'var', 'mw', 'kw', 'w', 'mvah', 'kvah', 'vah', 'mva',
    'kva', 'va', 'kv', 'v', 'ka', 'a', 'pf'):
        error_text = 'Invalid UOM encountered. Found value %s' % vals[6]
        print(error_text)
        return False, []
        # valid IntervalLength
    # if not vals[7] in (1, 5, 10, 15, 30):
    if not vals[7] in ('1', '5', '10', '15', '30'):
        error_text = 'Invalid IntervalLength value encountered. Found %d', vals[7]
        print(error_text)
        return False, []

    # insert record to database, returning id
    # sql = sql_mdff_merge_statement(table_name, fields + ['staging_100_id', 'source_file_id'], merge_keys)
    # curs = conn.cursor()
    # curs.execute(sql, tuple(vals) + (last100[-1], source_file_id))
    # thisid = curs.fetchone()[0]
    # curs.close()

    fields.append('Staging100Id')
    vals.append(last100[-1])
    fields.append('FileId')
    thisid = str(uuid.uuid4())
    vals.append(thisid)
    # print(vals)

    createCSV('staging200', fields, [vals])
    return True, vals


def process_MDF_250(version_header,toks,last_rec_ind,last100):
    return True, []

# ====
def process_MDF_300(version_header,toks,last_rec_ind,last100,last200):
    valid_after = ['200','300','400','500']
    table_name = 'NEMMDF_Staging_300'
    n_readings = int(1440 / int(last200[7]))
    qm = 1+n_readings
    fields = ['IntervalDate']+ ['IntervalValue'+str(i) for i in range(1,n_readings+1)] +['QualityMethod','ReasonCode','ReasonDescription','UpdateDateTime','MSATSLoadDateTime']
    db_fields = ['IntervalDate']+ ['IntervalValue'+str(i) for i in range(1,n_readings+1)] +['UpdateDateTime','MSATSLoadDateTime']

    data_types = 'D' + 'N'*n_readings + 'VNVDD'
    if last200[6][0].upper() == 'M':
        reading_length = 15.6
    elif last200[6][0].upper() == 'K':
        reading_length = 15.3
    elif last200[6][0].upper() == 'p':
        reading_length = 15.2
    else:
        reading_length = 15

    lengths = [8]+ [reading_length for i in range(1,n_readings+1)] +[3,3,240,14,14]
    mandatory = [True] + [True for i in range(1,n_readings+1)] +[True,False,False,False,False]

    if not last_rec_ind in valid_after:      # check for file blocking errors
        error_text = 'Meter data file blocking error'
        print (error_text)
        return (False,[])

    # allocate tokens, confirm data types and field lengths as required
    (status,vals) = mdf_length_type_check(toks,fields,data_types,lengths,mandatory)
    if not status:
        return (False,[])

    # specific checks

    # valid qualitymethod
    if toks[qm] not in ('A','N','V') and match(r"[AEFNSV][1567][1-9]",toks[qm]) is None:    # note: detects most but not all illegal values
        error_text = 'Invalid QualityMethod value in 300 row. Found %s'% toks[qm]
        print (error_text)
        return (False,[])
    # reasoncode valid if provided
    if len(toks[qm+1])>0 and (int(vals[qm+1]) < 0 or int(vals[qm+1])>94):
        error_text = 'Invalid ReasonCode supplied in 300 row. Found %s'% toks[qm+1]
        print (error_text)
        return (False,[])
    # no reasoncode if qualityflag is V
    if (vals[qm+1] is not None and toks[qm][0]=='V') or (vals[qm+1] is None and toks[qm][0] in ('F','S')):
        error_text = 'In 300 row, ReasonCode supplied with quality "V" or ReasonCode not supplied with Quality "F" or "S". Quality flag %s, ReasonCode %s'% (toks[qm][0], toks[qm+1])
        print (error_text)
        return (False,[])
    # reasondescription supplied if reasoncode = 0
    if len(vals[qm+2]) < 1 and vals[qm+1]==0:
        error_text = 'Missing ReasonDescription where ReasonCode is 0 in 300 row'
        print (error_text)
        return (False,[])
    # updatedatetime provided unless qualitymethod is N
    if vals[qm+3] is None and vals[qm][0] != 'N':
        error_text = 'Missing UpdateDateTime in 300 row where Quality is not "N"'
        print (error_text)
        return (False,[])

    # merge quality record and return id
    # sql = sql_mdff_merge_statement(table_name,db_fields+['staging_200_id','source_file_id'],[])
    # curs = conn.cursor()
    # insert_vals = vals[0:n_readings+1] + vals[n_readings+4:]
    # insert_vals[0] = str(insert_vals[0])
    # for t in range(1,n_readings+1):
    #     insert_vals[t] = float(insert_vals[t])
    # curs.execute(sql, tuple(insert_vals)+(last200[-1],source_file_id))
    # thisid = curs.fetchone()[0]
    # curs.close()
    # vals.append(thisid)

    insert_vals = vals[0:n_readings+1] + vals[n_readings+4:]
    # insert_vals[0] = str(insert_vals[0])
    # insert_vals[0] = insert_vals[0].strftime('%Y-%m-%d %H:%M')
    if isinstance(insert_vals[0], datetime):
        insert_vals[0] = insert_vals[0].strftime('%Y-%m-%d %H:%M')
    if isinstance(insert_vals[-1], datetime):
        insert_vals[-1] = insert_vals[-1].strftime('%Y-%m-%d %H:%M')
    if isinstance(insert_vals[-2], datetime):
        insert_vals[-2] = insert_vals[-2].strftime('%Y-%m-%d %H:%M')

    # for t in range(1,n_readings+1):
    #     insert_vals[t] = float(insert_vals[t])

    db_fields.append('Staging200Id')
    insert_vals.append(last200[-1])
    db_fields.append('FileId')
    thisid = str(uuid.uuid4())
    insert_vals.append(thisid)

    vals.append(thisid)

    # print(insert_vals)
    insert_vals = [str(val) for val in insert_vals]

    createCSV('staging300', db_fields, [insert_vals])

    # insert dummy 400 record to hold quality information
    if toks[qm][0] != 'V':
        (tf,res) = process_MDF_400(version_header,['1',str(n_readings),toks[qm],toks[qm+1],toks[qm+2]],'300',last100,last200,vals,None)
        if not tf:
            return (False,[])


    return (True,vals)




def process_MDF_400(version_header,toks,last_rec_ind,last100,last200,last300,last400):
    valid_after = ['300','400']
    table_name = 'NEMMDF_Staging_400'
    fields = ['StartInterval','EndInterval','QualityMethod','ReasonCode','ReasonDescription']

    data_types = 'NNVNV'
    lengths = [4,4,3,3,240]
    mandatory = [True,True,True,False,False]
    qm = 2

    if not last_rec_ind in valid_after:      # check for file blocking errors
        error_text = 'Meter data file blocking error'
        print (error_text)
        return (False,[])

    # allocate tokens, confirm data types and field lengths as required
    (status,vals) = mdf_length_type_check(toks,fields,data_types,lengths,mandatory)
    if not status:
        return (False,[])

    # specific checks
    vals[0] = int(vals[0])
    vals[1] = int(vals[1])
    last200[7] = int(last200[7])
    if vals[0] < 1 or vals[1] < vals[0] or vals[1] > 1440/last200[7]:
        error_text = 'Illegal StartInterval/EndInterval values. StartInterval = %d, EndInterval = %d, IntervalLength = %d'% vals[0], vals[1],last200[7]
        print (error_text)
        return (False,[])
    if (last_rec_ind != '400' and vals[0] != 1) or (last_rec_ind == '400' and vals[0] != last400[1]+1):
        error_text = 'Mismatch between StartInterval and preceeding row in 400-record'
        print (error_text)
        return (False,[])
    # valid qualitymethod
    if toks[qm] not in ('A','N') and match(r"[AEFNS][1567][1-9]",toks[qm]) is None:    # note: detects most but not all illegal values
        error_text = 'Invalid QualityMethod value in 400 row. Found %s'% toks[qm]
        print (error_text)
        return (False,[])
    # reasoncode valid if provided
    if len(toks[qm+1])>0 and (int(vals[qm+1]) < 0 or int(vals[qm+1])>94):
        error_text = 'Invalid ReasonCode supplied in 400 row. Found %s'% toks[qm+1]
        print (error_text)
        return (False,[])
    # no reasoncode if qualityflag is V
    if (vals[qm+1] is not None and toks[qm][0]=='V') or (vals[qm+1] is None and toks[qm][0] in ('F','S')):
        error_text = 'In 400 row, ReasonCode supplied with quality "V" or ReasonCode not supplied with Quality "F" or "S". Quality flag %s, ReasonCode %s'% toks[qm][0], toks[qm+1]
        print (error_text)
        return (False,[])
    # reasondescription supplied if reasoncode = 0
    if len(vals[qm+2]) < 1 and vals[qm+1]==0:
        error_text = 'Missing ReasonDescription where ReasonCode is 0 in 300 row'
        print (error_text)
        return (False,[])
    # last300 had qualityflag V, or its a single-record 400 row

    if last300[-6][0] != "V" and (vals[0] != 1 or vals[1] != 1440/last200[7]):
        error_text = '400-record found after 300-row with quality not V'
        print(error_text)
        return (False,[])


    # merge quality record to database, returning id
    # sql = sql_mdff_merge_statement(table_name,fields+['staging_300_id','source_file_id'],[])
    # curs = conn.cursor()
    # curs.execute(sql, tuple(vals)+(last300[-1],source_file_id))
    # thisid = curs.fetchone()[0]
    # curs.close()
    # vals.append(thisid)

    fields.append('Staging300Id')
    vals.append(last300[-1])
    fields.append('FileId')
    thisid = str(uuid.uuid4())
    vals.append(thisid)
    # print(vals)
    vals = [str(val) for val in vals]

    createCSV('staging400', fields, [vals])
    return True, vals


def process_MDF_500(version_header,toks,last_rec_ind,last100,last200,last300,last400):
    valid_after = ['300','400','500']
    table_name = 'NEMMDF_Staging_500'
    fields = ['TransCode','RetServiceOrder','ReadDateTime','IndexRead']
    data_types = 'CVDV'
    lengths = [1,15,14,15]
    mandatory = [True,False,False,False]

    if not last_rec_ind in valid_after:      # check for file blocking errors
        error_text = 'Meter data file blocking error'
        print (error_text)
        return (False,[])

    # allocate tokens, confirm data types and field lengths as required
    (status,vals) = mdf_length_type_check(toks,fields,data_types,lengths,mandatory)
    if not status:
        return (False,[])

    # specific checks
    if vals[0] not in ("A","C","G","D","E","N","O","S","R"):
        error_text = 'vals[0] not in "A","C","G","D","E","N","O","S","R"'
        print (error_text)
        return (False,[])


    # insert record to database, returning id

    # sql = sql_mdff_merge_statement(table_name,fields+['staging_300_id','source_file_id'],[])
    # curs = conn.cursor()
    # curs.execute(sql, tuple(vals+[last300[-1],source_file_id]))
    # thisid = curs.fetchone()[0]
    # curs.close()
    # vals.append(thisid)
    fields.append('Staging300Id')
    vals.append(last300[-1])
    fields.append('FileId')
    thisid = str(uuid.uuid4())
    vals.append(thisid)
    # print(vals)
    # print (vals[2])
    # print (type(vals[2]))
    if isinstance(vals[2], datetime):
        vals[2] = vals[2].strftime('%Y-%m-%d %H:%M')
    vals = [str(val) for val in vals]


    createCSV('staging500', fields, [vals])

    return (True,vals)


def process_MDF_900(version_header,toks,last_rec_ind):
    valid_after = ['300','400','500']
    if not last_rec_ind in valid_after:      # check for file blocking errors
        error_text = 'Meter data file blocking error'
        print (error_text)
        return (False,[])

    # curs = conn.cursor()
    # curs.execdirect('EXEC MeterDataDB.dbo.MergeNEMMDF_Staging '+ str(source_file_id) )
    # curs.close()
    # conn.commit()

    return (True,[])

# ====!


def process_MDF_550(version_header,toks,last_rec_ind,last250):
    return True, []

def handler(fname):
    (filename, fileext) = os.path.splitext(fname)

    # determine if file has a valid filename for a NEM12 or NEM13 file
    s = filename.split('#')
    if len(s) == 4 and s[0] in ("NEM12","NEM13") and len(s[1])<=36 and len(s[2])<=10 and len(s[3])<=10:
        (version_header,sender_id_val,from_participant,to_participant) = s
    else:
        print('Invalid file name encountered %s. Expecting NEMXX#IDENTIFIER_LEN36#FROMPARTIC#TOPARTICIP', filename)
        version_header = sender_id_val = from_participant = to_participant = None

    # process file
    with open(fname, 'rt') as f:
        line_number = 0
        last_rec_ind = None
        last100 = last200 = last250 = last300 = last400 = None

        while last_rec_ind != '900':
            toks = f.readline().strip().split(',')    # read and split next line
            rec_ind = toks[0].strip()

            if rec_ind == '100':
                status,last100 = process_MDF_100(version_header,toks[1:],last_rec_ind)
            elif rec_ind == '200':
                status, last200 = process_MDF_200(version_header, toks[1:], last_rec_ind, last100)
            elif rec_ind == '250':
                status,last250 = process_MDF_250(version_header,toks[1:],last_rec_ind,last100)
            # khoa added
            elif rec_ind == '300':
                status,last300 = process_MDF_300(version_header,toks[1:],last_rec_ind,last100,last200)
            elif rec_ind == '400':
                status,last400 = process_MDF_400(version_header,toks[1:],last_rec_ind,last100,last200,last300,last400)
            # elif rec_ind == '500':
            #     status,res = process_MDF_500(conn,version_header,toks[1:],last_rec_ind,last100,last200,last300,last400,source_file_id)
            elif rec_ind == '500':
                status,res = process_MDF_500(version_header,toks[1:],last_rec_ind,last100,last200,last300,last400)
            # end khoa added
            elif rec_ind == '550':
                status,res = process_MDF_550(version_header,toks[1:],last_rec_ind,last100,last250)
            elif rec_ind == '900':
                status,res = process_MDF_900(version_header,toks[1:],last_rec_ind)
            else:
                error_text = 'Meter data file error. Invalid record indicator found in line %d: "%s"' % (line_number, rec_ind)
                print(error_text)
                status = False

            if status == False:
                error_text = 'Error encountered processing file %s. The error occurred at line number %d' % (fname, line_number)
                print(error_text)
                return False, 0

            # prepare for next iteration of loop
            last_rec_ind = rec_ind
            line_number = line_number + 1

fname = 'nem12#0000022860#TCAUSTM#SAVVYPLUS.csv'
handler(fname)
