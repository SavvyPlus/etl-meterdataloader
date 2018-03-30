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
    print(vals)
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
