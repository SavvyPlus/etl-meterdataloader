# start
NEMMDF_FileDetails
VersrionHeader only NEM12
Can do it now
get from nemreader header
print('Header:', m.header)
100
# end

# start
NEMMDF_UOM
Satic table
Can do it now
Just upload current table to S3
# end

# start
NEMMDF_Stream
ID	NMI	NMISuffix	MeterPointRef	StreamRef
1	6203861487	E1	1	E
266	6102913300	EA	A	E

for nmi in m.readings:
    for channel in m.readings[nmi]:
        print(nmi, 'Channel', channel)
        for reading in m.readings[nmi][channel][-rows:]:
            print('', reading)

NMISuffix = for channel in m.readings[nmi]
MeterPointRef = NMISuffix[1]
StreamRef = NMISuffix[0]

Do before NEMMDF_B2BDetailes
Can do it now
get from print('Transactions:', m.transactions) and produce NEMMDF_B2BDetailes

# TO ASK join NEMMDF_Stream and NEMMDF_B2BDetailes in to one table
# end


# start
NEMMDF_B2BDetailes
500
# end

# start
NEMMDF_StreamDetails
ID	NMIConfiguration	RegisterID	MDMDataStreamIdentifier	MeterSerialNumber	UOM	IntervalLength	NextScheduledReadDate
200
# end

# start
NEMMDF_QualityDetails
ID	QualityMethod	ReasonCode	ReasonDescription
400
# end

# start
NEMMDF_StreamDay
300
#end

# start
NEMMDF_IntervalData
interval inside 300
# end
