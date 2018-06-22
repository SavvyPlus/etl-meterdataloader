DEFAULT_TRANS_CODE = "A"


HEADERS = {
    "NEMMDF_FileDetails": "ID	VersionHeader	DateTime	FromParticipant	ToParticipant	source_file_id".split(),

    "NEMMDF_B2BDetailes": "ID	StreamID	IntervalDate	TransCode	RetServiceOrder	ReadDateTime	IndexRead".split(),

    "NEMMDF_IntervalData": "StreamID	IntervalDate	IntervalNumber	IntervalLength	Value	UOM_ID	QualityDetailsID".split(),

    "NEMMDF_QualityDetails": "ID	QualityMethod	ReasonCode	ReasonDescription".split(),

    "NEMMDF_Stream": "ID	NMI	NMISuffix	MeterPointRef	StreamRef".split(),

    "NEMMDF_StreamDay": "StreamID	IntervalDate	source_file_id	UpdateDateTime	MSATSLoadDateTime	\
                        StreamDetailsID	IntervalLength".split(),

    "NEMMDF_StreamDetails": "ID	NMIConfiguration	RegisterID	MDMDataStreamIdentifier	MeterSerialNumber \
    	                    UOM	IntervalLength	NextScheduledReadDate".split()

}
