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

    print(vals)
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
    print(vals)

    createCSV('staging200', fields, [vals])
    return True, vals


def process_MDF_250(version_header,toks,last_rec_ind,last100):
    return True, []


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
            elif rec_ind == '550':
                status,res = process_MDF_550(version_header,toks[1:],last_rec_ind,last100,last250)
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

fname = 'NEM12#16042100299000000#GLOBALM#SPICK.csv'
handler(fname)