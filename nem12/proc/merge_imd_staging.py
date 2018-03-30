"""
MergeIMD_Staging
"""


def delete_invalid_imd_staging():
    """
    DELETE FROM IMD_Staging WHERE source_file_id IN
	(
		SELECT DISTINCT source_file_id FROM IMD_Staging
		WHERE
		IntervalLength NOT IN (15, 30)
		OR (PeriodID NOT BETWEEN 1 and 96)
		OR (PeriodID > 48 AND IntervalLength = 30)
		OR (Exp_KWH < 0)
		OR (Imp_KWH < 0)
		OR (Exp_KVARH < 0)
		OR (Exp_KVARH < 0)
		OR (KVA < 0)
		OR (QualityCode NOT IN ('A','E','F','S','X'))		-- Actual, Estimated, Final, Substituted, Unknown
	)
    """
    pass

# calculations
"""
-- Net_KWH from Exp_KWH and Imp_KWH
UPDATE [dbo].[IMD_Staging] SET Net_KWH = Exp_KWH - Imp_KWH WHERE Net_KWH IS NULL
-- Net_KVARH from Exp_KVARH and Imp_KVARH
UPDATE [dbo].[IMD_Staging] SET Net_KVARH = Exp_KVARH - Imp_KVARH WHERE Net_KVARH IS NULL
-- KW FROM Net_KWH
UPDATE [dbo].[IMD_Staging] SET KW = Net_KWH*(60/IntervalLength) WHERE KW IS NULL
-- Net_KWH from KW
UPDATE [dbo].[IMD_Staging] SET Net_KWH = KW/(60/IntervalLength) WHERE Net_KWH IS NULL
-- KVA from Net_KWH and Net_KVARH
UPDATE [dbo].[IMD_Staging] SET KVA = SQRT(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/IntervalLength) WHERE KVA IS NULL
"""

def merger_meter_point():
    """
    -- Add new MeterPoints
	MERGE IMD_MeterPoint T
	USING (
		SELECT DISTINCT MeterRef FROM IMD_Staging
	) AS S ON S.MeterRef = T.MeterRef
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (MeterRef, Commodity, [Type], [Source])
		VALUES (S.MeterRef, 'ELEC', 'UNKNOWN', 'UNKNOWN')	-- TODO: configure this properly
	;
    """
    pass

def update_meter_point():
    """
    UPDATE IMD_Staging SET MeterPointID = MP.ID
	FROM IMD_Staging S INNER JOIN IMD_MeterPoint MP ON MP.MeterRef = S.MeterRef
    """
    pass

def remove_duplicate():
    """
    -- Deal with duplicates in staging table
	-- TODO: improve the logic here - for now just grab the data for the most recent source_file_id
	DELETE S FROM IMD_Staging S INNER JOIN (
		SELECT MeterPointID, [Date], PeriodID, max(source_file_id) as source_file_id
		FROM IMD_Staging GROUP BY MeterPointID, [Date], PeriodID
	) Smax ON S.MeterPointID = Smax.MeterPointID AND S.Date = Smax.Date AND S.PeriodID = Smax.PeriodID AND S.source_file_id <> Smax.source_file_id
    """
    pass


"""
merge to 15 and 30
DELETE FROM IMD_Staging
"""
