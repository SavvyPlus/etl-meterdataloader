"""
StoredProcedure [dbo].[MergeNEMMDF_Staging]
"""


"""
DECLARE @MergeOutput TABLE (
	id1 int,
	id2 int
)

DECLARE @StreamDaysToUpdate TABLE (
	IntervalLength tinyint,
	StreamID int,
	IntervalDate date
)

DECLARE @DeletedDays TABLE (
	StreamID int,
	IntervalDate date
)
"""


def merge_nmi_stream():
    """
    EXEC PerfLog @sfid, 'merge NMI streams';
    -- Merge NMI Streams
    input is NEMMDF_Staging_200 table (csv)
    INSERT (NMI, NMISuffix, MeterPointRef, StreamRef)
    VALUES (src.NMI, src.NMISuffix, right(src.NMISuffix,1), left(src.NMISuffix,1))

    distinct (NMI,NMISuffix) from NEMMDF_Staging_200

    200 -> nmi_stream
    """
    pass


def merge_nmi_stream_detail():
    """
    EXEC PerfLog @sfid, 'merge stream details';
    MERGE NEMMDF_StreamDetails t
    NEMMDF_Staging_200 -> NEMMDF_StreamDetails
    -> src.staging_200_id, inserted.ID INTO @MergeOutput;
    src NEMMDF_Staging_200
    """
    pass


def update_staging_200_stream_detail():
    """
    EXEC PerfLog @sfid, 'update staging stream details';
    UPDATE NEMMDF_Staging_200 SET StreamDetailsID = B.ID
    FROM NEMMDF_Staging_200 A INNER JOIN NEMMDF_StreamDetails B ON A.NMIConfiguration = B.NMIConfiguration
    AND A.UOM = B.UOM AND A.IntervalLength = B.IntervalLength AND isnull(A.RegisterID, '') = isnull(B.RegisterID, '')
    AND isnull(A.MDMDataStreamIdentifier, '') = isnull(B.MDMDataStreamIdentifier, '')
    AND isnull(A.MeterSerialNumber, '') = isnull(B.MeterSerialNumber, '')
    AND ((A.NextScheduledReadDate is null and B.NextScheduledReadDate is null)
    OR A.NextScheduledReadDate = B.NextScheduledReadDate)

    update 200 based on NEMMDF_StreamDetails
    """
    pass


def update_staging_200_streamid_uomid():
    """
    EXEC PerfLog @sfid, 'update staging table streamid and uom_id';

    update 200 based on NEMMDF_Stream and NEMMDF_UOM (may be pre-defined)
    """
    pass


def merge_quality_details():
    """
    EXEC PerfLog @sfid, 'merge quality details';

    (QualityMethod, ReasonCode, ReasonDescription)
    **OUTPUT inserted.ID;
    400 -> NEMMDF_QualityDetails
    """
    pass


def update_staging_400_quality_detail():
    """
    EXEC PerfLog @sfid, 'update staging quality details';

    update 400 based on NEMMDF_QualityDetails
    """
    pass


def determine_stream_days_to_update():
    """
    EXEC PerfLog @sfid, 'determine stream days to update';

    insert to @StreamDaysToUpdate based on NEMMDF_StreamDay, 200, 300
    NEMMDF_StreamDay (will be merged later)
    """
    pass


def determine_stream_days_to_insert():
    """
    insert to @StreamDaysToUpdate based on NEMMDF_StreamDay, 200, 300
    NEMMDF_StreamDay (will be merged later)
    """
    pass


def merge_file_details_to_main_table():
    """
    EXEC PerfLog @sfid, 'merge file details to main table';

    merger to NEMMDF_FileDetails from NEMMDF_Staging_100
    """
    pass


def delete_old_interval_data():
    """
    EXEC PerfLog @sfid, 'delete old interval data';
    DELETE FROM [NEMMDF_Staging_IntervalData];
    """
    pass


def construct_interval_data():
    """
    EXEC PerfLog @sfid, 'construct interval data';

    300, 200, @StreamDaysToUpdate do INNER JOIN -> #A
    #A -> NEMMDF_Staging_IntervalData
    """
    pass

def add_quality_to_interval_data():
    """
    EXEC PerfLog @sfid, 'add quality details to staging interval data';

    update NEMMDF_Staging_IntervalData based on
    NEMMDF_Staging_IntervalData and NEMMDF_Staging_400
    """
    pass

def merge_stream_days():
    """
    EXEC PerfLog @sfid, 'merge stream days';

    merge NEMMDF_StreamDay based on 200, 300
    -> OUTPUT deleted.StreamID, deleted.IntervalDate INTO @DeletedDays;
    """
    pass

def delete_old_b2b_detail():
    """
    DELETE B FROM [dbo].[NEMMDF_B2BDetails] B
    INNER JOIN @DeletedDays D ON D.StreamID = B.StreamID AND D.IntervalDate = B.IntervalDate
    """
    pass

def insert_new_b2b_detail():
    """
    insert to NEMMDF_B2BDetails based on
    500, 300, 200
    """
    pass

def merge_inteval_data():
    """
    EXEC PerfLog @sfid, 'merge interval data';
    merge NEMMDF_IntervalData based on NEMMDF_Staging_IntervalData and @StreamDaysToUpdate
    """
    pass

def delete_leftover_interval_data():
    """
    DELETE I FROM NEMMDF_IntervalData I
    INNER JOIN @StreamDaysToUpdate D ON I.StreamID = D.StreamID AND I.IntervalDate = D.IntervalDate
    WHERE D.IntervalLength = 30 and I.IntervalNumber > 48
    """
    pass

"""
EXEC PerfLog @sfid, 'deleting from staging tables';
DELETE FROM [dbo].[NEMMDF_Staging_100] WHERE source_file_id = @sfid
DELETE FROM [dbo].[NEMMDF_Staging_200] WHERE source_file_id = @sfid
DELETE FROM [dbo].[NEMMDF_Staging_300] WHERE source_file_id = @sfid
DELETE FROM [dbo].[NEMMDF_Staging_400] WHERE source_file_id = @sfid
DELETE FROM [dbo].[NEMMDF_Staging_500] WHERE source_file_id = @sfid
"""

"""
EXEC PerfLog @sfid, 'sending to IMD_Staging table';
-- IMD Staging data
DECLARE @nmi varchar(10)
DECLARE @firstdate date
DECLARE @lastdate date
DECLARE @nmi_dr_cursor CURSOR;

BEGIN
	SET @nmi_dr_cursor = CURSOR FOR
	SELECT S.NMI, MIN(SD.IntervalDate) AS FirstDate, MAX(SD.IntervalDate) AS LastDate
	FROM @StreamDaysToUpdate SD INNER JOIN [dbo].[NEMMDF_Stream] S ON SD.StreamID = S.ID
	GROUP BY S.NMI

	OPEN @nmi_dr_cursor
	FETCH NEXT FROM @nmi_dr_cursor INTO @nmi, @firstdate, @lastdate

	WHILE @@FETCH_STATUS = 0
	BEGIN
		FETCH NEXT FROM @nmi_dr_cursor INTO @nmi, @firstdate, @lastdate;
		EXEC [dbo].[NEMMDF_to_IMD_Staging] @nmi, @firstdate, @lastdate;
		EXEC PerfLog @sfid, @NMI
	END;

	CLOSE @nmi_dr_cursor;
	DEALLOCATE @nmi_dr_cursor;
END
"""

# call another stored procs
"""
EXEC PerfLog @sfid, 'merge to IMD tables';
EXEC MergeIMD_Staging


EXEC PerfLog @sfid, 'ends';
"""
