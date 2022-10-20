#Author Dane de Beer
#Script to pull in data from onprem PostgreSQL server to Azure SQL Server using Python
#all transformations happens in Azure SQL Server

import psycopg2
import pandas as pd
import datetime
from datetime import datetime, timedelta
from datetime import date , time
import datetime
import calendar
import pyodbc as dbc
import numpy 
from numpy import isnan

Host = 'host'
port ='port'
pusername = 'username'
ppassword = 'password'
pdatabase = 'database'
tables = list()
tables = 'cv001_lynxx_001','cv002_lynxx_001','cv005_lynxx_001','cv010_lynxx_001','cv015_lynxx_001','cv023_lynxx_001','cv504_lynxx_001','cv504_lynxx_002','cv512_lynxx_001','cv513_lynxx_001'
global db123
cv=''

server ='server2'
database ='database2'
username = 'database2'
password = 'password2'
sqlgetDate = """SELECT TOP 1 [TimeStamp] FROM [dbo].[Pcs_PSDAnalysers_Hourly] WHERE CV = '""" + cv + """' ORDER BY [TimeStamp] DESC"""
conn2 = dbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';PORT=1433;Database=' + database + ';UID=' + username + ';PWD=' + password)

# Define Function Chunks for insert
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Define Function to Insert data into SQL
def InsertIntoSQL(data_set, sql_insert_statement):
    # Delcare chunk size for insert
    chunks_size = 5000

    cursor2 = conn2.cursor()
    cursor2.fast_executemany = True

    # Insert Data in chunks into SQL
    for data_chunk in chunks(data_set, chunks_size):
        cursor2.executemany(sql_insert_statement, data_chunk)
        conn2.commit()




# connect to the PostgreSQL server
print('Connecting to the PostgreSQL database...')
conn = psycopg2.connect(host=Host, port = port, database=pdatabase, user=pusername, password="postgres")
print('Done connection to postgress')		
# create a cursor
cur = conn.cursor()

# execute a statement
print('Reading database')
dbvalues_list = list()


for i in range(len(tables)):
    data = list()
    cur = conn.cursor()
    cursor2 = conn2.cursor()

    if tables[i] == '001_lynxx_001':
        cv = 'CV001'
    if tables[i] == '002_lynxx_001':
        cv = 'CV002'
    if tables[i] == '003_lynxx_001':
        cv = 'CV003'
    if tables[i] == '005_lynxx_001':
        cv = 'CV005'
    if tables[i] == '010_lynxx_001':
        cv = 'CV010'
    if tables[i] == '015_lynxx_001':
        cv = 'CV015'
    if tables[i] == '023_lynxx_001':
        cv = 'CV023'
    if tables[i] == '504_lynxx_001':
        cv = 'CV504_1'
    if tables[i] == '504_lynxx_002':
        cv = 'CV504_2'
    if tables[i] == '512_lynxx_001':
        cv = 'CV512'
    if tables[i] == '513_lynxx_001':
		
        cv = 'CV513'

    sqlgetDate = """SELECT TOP 1 [TimeStamp] FROM [dbo].Pcs_PSDAnalysers_Hourly WHERE CV = '""" + cv + """' ORDER BY [TimeStamp] DESC"""
    print(sqlgetDate)
    cursor2.execute(sqlgetDate)
    datelast = cursor2.fetchone()
    print(datelast)
    booldate = str(datelast)
    if booldate == 'None':
        dateOFEnd = '2021-11-11 00:00:00'
    else:
        dateOFEnd = datelast[0] - timedelta(1)
        #dateOFEnd = '2022-01-01 00:00:00'
    print(dateOFEnd)
    #dateOFEnd = '2021-11-01'
    
    print(f"SELECT * FROM {tables[i]} WHERE all_ok_0 = 1 AND when_captured >= '{dateOFEnd}' ORDER BY when_captured desc ")
    cur.execute(f"SELECT * FROM {tables[i]} WHERE all_ok_0 = 1 AND when_captured >= '{dateOFEnd}' ORDER BY when_captured desc " )
    db = cur.fetchall() 
                    
    for db_i in range(len(db)):
        dbvalues = (list(db[db_i]) + list([cv]))
        dbvalues_list.append(tuple(dbvalues))

sql = 'INSERT INTO #temp_Analysers VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
merge = """DECLARE @table TABLE
       (HourStart TIME, HourEnd TIME, ID INT, AddDays INT, ActualHour TIME)
INSERT INTO @table VALUES
('00:00:00', '00:59:59', 1, 0, '01:00:00'),('01:00:00', '01:59:59', 2, 0, '02:00:00'),('02:00:00', '02:59:59', 3, 0, '03:00:00'),('03:00:00', '03:59:59', 4, 0, '04:00:00'),('04:00:00', '04:59:59', 5, 0, '05:00:00'),('05:00:00', '05:59:59', 6, 0, '06:00:00'),
('06:00:00', '06:59:59', 7, 0, '07:00:00'),('07:00:00', '07:59:59', 8, 0, '08:00:00'),('08:00:00', '08:59:59', 9, 0, '09:00:00'),('09:00:00', '09:59:59', 10, 0, '10:00:00'),('10:00:00', '10:59:59', 11, 0, '11:00:00'),('11:00:00', '11:59:59', 12, 0, '12:00:00'),
('12:00:00', '12:59:59', 13, 0, '13:00:00'),('13:00:00', '13:59:59', 14, 0, '14:00:00'),('14:00:00', '14:59:59', 15, 0, '15:00:00'),('15:00:00', '15:59:59', 16, 0, '16:00:00'),('16:00:00', '16:59:59', 17, 0, '17:00:00'),('17:00:00', '17:59:59', 18, 0, '18:00:00'),
('18:00:00', '18:59:59', 19, 0, '19:00:00'),('19:00:00', '19:59:59', 20, 0, '20:00:00'),('20:00:00', '20:59:59', 21, 0, '21:00:00'),('21:00:00', '21:59:59', 22, 0, '22:00:00'),('22:00:00', '22:59:59', 23, 0, '23:00:00'),('23:00:00', '23:59:59', 0, 1, '00:00:00')

;
WITH
       HourTable AS
              (SELECT * FROM @table),
       MergeHourTable AS
              (SELECT hh.*, psd.*  FROM #temp_Analysers psd CROSS APPLY HourTable hh
              WHERE (CAST(when_captured AS TIME) BETWEEN HourStart AND HourEnd) AND all_ok_0 = 1),
       NewTimes AS
              (SELECT
                     CAST(CONCAT(DATEADD(DAY, AddDays, CAST(when_captured AS DATE)), ' ', LEFT(ActualHour, 8)) AS DATETIME) AS TimeStamp, MergeHourTable.* FROM MergeHourTable),
       Averages (TimeStamp,
              sievesize_0, sievesize_1, sievesize_2, sievesize_3, sievesize_4, sievesize_5, sievesize_6, sievesize_7, sievesize_8, sievesize_9, sievesize_10,
              sievesize_11, sievesize_12, sievesize_13, sievesize_14, sievesize_15, sievesize_16, sievesize_17, sievesize_18, sievesize_19, smp_0, smp_1, smp_2,
              smp_3, smp_4, smp_5, smp_6, smp_7, smp_8, smp_9, smp_10, smp_11, smp_12, smp_13, smp_14, smp_15, smp_16, smp_17, smp_18, smp_19, sfcp_0, sfcp_1,
              sfcp_2, sfcp_3, sfcp_4, sfcp_5, sfcp_6, sfcp_7, sfcp_8, sfcp_9, sfcp_10, sfcp_11, sfcp_12, sfcp_13, sfcp_14, sfcp_15, sfcp_16, sfcp_17, sfcp_18,
              sfcp_19, sfp_0, sfp_1, sfp_2, sfp_3, sfp_4, sfp_5, sfp_6, sfp_7, sfp_8, sfp_9, sfp_10, sfp_11, sfp_12, sfp_13, sfp_14, sfp_15, sfp_16, sfp_17,
              sfp_18, sfp_19, mp_0, mp_1, mp_2, mp_3, mp_4, mp_5, mp_6, mp_7, mp_8, mp_9, mp_10, mp_11, mp_12, mp_13, mp_14, mp_15, mp_16, mp_17, mp_18, mp_19,
              ap_0, ap_1, ap_2, ap_3, ap_4, ap_5, ap_6, ap_7, ap_8, ap_9, ap_10, ap_11, ap_12, ap_13, ap_14, ap_15, ap_16, ap_17, ap_18, ap_19, csfcp_0, csfcp_1,
              csfcp_2, csfcp_3, csfcp_4, csfcp_5, csfcp_6, csfcp_7, csfcp_8, csfcp_9, csfcp_10, csfcp_11, csfcp_12, csfcp_13, csfcp_14, csfcp_15, csfcp_16, csfcp_17,
              csfcp_18, csfcp_19, ratio_0, ratio_1, ratio_2, ratio_3, ratio_4, ratio_5, ratio_6, ratio_7, ratio_8, ratio_9, ratio_10, ratio_11, ratio_12, ratio_13,
              ratio_14, ratio_15, ratio_16, ratio_17, ratio_18, ratio_19, p20_p50_p80_0, p20_p50_p80_1, p20_p50_p80_2, raw_p20_p50_p80_0, raw_p20_p50_p80_1,
              raw_p20_p50_p80_2, beltevents_0, beltevents_1, beltevents_2, beltevents_3, beltevents_4, beltevents_5, beltevents_6, beltevents_7, oversizeevents_0,
              oversizeevents_1, oversizeevents_2, oversizeevents_3, CV, top_size_0) AS

                     (SELECT
                           TimeStamp, 
                           AVG(sievesize_0), AVG(sievesize_1), AVG(sievesize_2), AVG(sievesize_3), AVG(sievesize_4), AVG(sievesize_5), AVG(sievesize_6), AVG(sievesize_7),
                           AVG(sievesize_8), AVG(sievesize_9), AVG(sievesize_10), AVG(sievesize_11), AVG(sievesize_12), AVG(sievesize_13), AVG(sievesize_14), AVG(sievesize_15),
                           AVG(sievesize_16), AVG(sievesize_17), AVG(sievesize_18), AVG(sievesize_19),
                           AVG(smp_0), AVG(smp_1), AVG(smp_2), AVG(smp_3), AVG(smp_4), AVG(smp_5), AVG(smp_6), AVG(smp_7), AVG(smp_8), AVG(smp_9), AVG(smp_10), AVG(smp_11),
                           AVG(smp_12), AVG(smp_13), AVG(smp_14), AVG(smp_15), AVG(smp_16), AVG(smp_17), AVG(smp_18), AVG(smp_19),
                           AVG(sfcp_0), AVG(sfcp_1), AVG(sfcp_2), AVG(sfcp_3), AVG(sfcp_4), AVG(sfcp_5), AVG(sfcp_6), AVG(sfcp_7), AVG(sfcp_8), AVG(sfcp_9), AVG(sfcp_10),
                           AVG(sfcp_11), AVG(sfcp_12), AVG(sfcp_13), AVG(sfcp_14), AVG(sfcp_15), AVG(sfcp_16), AVG(sfcp_17), AVG(sfcp_18), AVG(sfcp_19),
                           AVG(sfp_0), AVG(sfp_1), AVG(sfp_2), AVG(sfp_3), AVG(sfp_4), AVG(sfp_5), AVG(sfp_6), AVG(sfp_7), AVG(sfp_8), AVG(sfp_9), AVG(sfp_10), AVG(sfp_11),
                           AVG(sfp_12), AVG(sfp_13), AVG(sfp_14), AVG(sfp_15), AVG(sfp_16), AVG(sfp_17), AVG(sfp_18), AVG(sfp_19),
                           AVG(mp_0), AVG(mp_1), AVG(mp_2), AVG(mp_3), AVG(mp_4), AVG(mp_5), AVG(mp_6), AVG(mp_7), AVG(mp_8), AVG(mp_9), AVG(mp_10), AVG(mp_11), AVG(mp_12),
                           AVG(mp_13), AVG(mp_14), AVG(mp_15), AVG(mp_16), AVG(mp_17), AVG(mp_18), AVG(mp_19),
                           AVG(ap_0), AVG(ap_1), AVG(ap_2), AVG(ap_3), AVG(ap_4), AVG(ap_5), AVG(ap_6), AVG(ap_7), AVG(ap_8), AVG(ap_9), AVG(ap_10), AVG(ap_11), AVG(ap_12),
                           AVG(ap_13), AVG(ap_14), AVG(ap_15), AVG(ap_16), AVG(ap_17), AVG(ap_18), AVG(ap_19),
                           AVG(csfcp_0), AVG(csfcp_1), AVG(csfcp_2), AVG(csfcp_3), AVG(csfcp_4), AVG(csfcp_5), AVG(csfcp_6), AVG(csfcp_7), AVG(csfcp_8), AVG(csfcp_9),
                           AVG(csfcp_10), AVG(csfcp_11), AVG(csfcp_12), AVG(csfcp_13), AVG(csfcp_14), AVG(csfcp_15), AVG(csfcp_16), AVG(csfcp_17), AVG(csfcp_18), AVG(csfcp_19),
                           AVG(ratio_0), AVG(ratio_1), AVG(ratio_2), AVG(ratio_3), AVG(ratio_4), AVG(ratio_5), AVG(ratio_6), AVG(ratio_7), AVG(ratio_8), AVG(ratio_9),
                           AVG(ratio_10), AVG(ratio_11), AVG(ratio_12), AVG(ratio_13), AVG(ratio_14), AVG(ratio_15), AVG(ratio_16), AVG(ratio_17), AVG(ratio_18), AVG(ratio_19),
                           AVG(p20_p50_p80_0), AVG(p20_p50_p80_1), AVG(p20_p50_p80_2),
                           AVG(raw_p20_p50_p80_0), AVG(raw_p20_p50_p80_1), AVG(raw_p20_p50_p80_2),
                           AVG(beltevents_0), AVG(beltevents_1), AVG(beltevents_2), AVG(beltevents_3), AVG(beltevents_4), AVG(beltevents_5), AVG(beltevents_6), AVG(beltevents_7),
                           AVG(oversizeevents_0), AVG(oversizeevents_1), AVG(oversizeevents_2), AVG(oversizeevents_3),CV, AVG(top_size_0)

                     FROM NewTimes GROUP BY TimeStamp, CV),

		

       sieve AS

              (SELECT TimeStamp, SUBSTRING(sieveIndex, 11, 12) AS sieveIndex, sieveSize, CV FROM Averages
              UNPIVOT       (sieveSize FOR sieveIndex IN
                     ([sievesize_0], [sievesize_1], [sievesize_2], [sievesize_3], [sievesize_4], [sievesize_5], [sievesize_6],
                     [sievesize_7], [sievesize_8], [sievesize_9], [sievesize_10], [sievesize_11], [sievesize_12], [sievesize_13],
                     [sievesize_14], [sievesize_15], [sievesize_16], [sievesize_17], [sievesize_18], [sievesize_19])) sieve),

       smp AS

              (SELECT TimeStamp,  SUBSTRING(smpIndex, 5, 7) AS smpIndex, smp, CV FROM Averages
              UNPIVOT(smp FOR smpIndex IN
                     ([smp_0], [smp_1], [smp_2], [smp_3], [smp_4], [smp_5], [smp_6], [smp_7], [smp_8], [smp_9], [smp_10], [smp_11],
                     [smp_12], [smp_13], [smp_14], [smp_15], [smp_16], [smp_17], [smp_18], [smp_19])) smp),

		sfcp AS
				(SELECT TimeStamp, SUBSTRING(sfcpIndex,6,8) AS sfcpIndex, sfcp, CV FROM Averages
				UNPIVOT     (sfcp FOR sfcpIndex IN
							([sfcp_0], [sfcp_1],[sfcp_2], [sfcp_3], [sfcp_4], [sfcp_5], [sfcp_6], [sfcp_7], [sfcp_8], [sfcp_9], [sfcp_10], [sfcp_11], [sfcp_12], [sfcp_13], [sfcp_14],
							[sfcp_15], [sfcp_16], [sfcp_17], [sfcp_18],[sfcp_19]))sfcp),

		sfp AS
				(SELECT TimeStamp, SUBSTRING(sfpIndex,5,7) AS sfpIndex, sfp,CV FROM Averages
				UNPIVOT		(sfp FOR sfpIndex IN
							( [sfp_0], [sfp_1], [sfp_2], [sfp_3], [sfp_4], [sfp_5], [sfp_6], [sfp_7], [sfp_8],[sfp_9], [sfp_10], [sfp_11], [sfp_12], [sfp_13], [sfp_14], 
							[sfp_15], [sfp_16], [sfp_17],[sfp_18], [sfp_19]))sfp),
		mp AS
				(SELECT TimeStamp, SUBSTRING(mpIndex,4,6) AS mpIndex, mp,CV FROM Averages
				UNPIVOT		(mp FOR mpIndex IN
							([mp_0], [mp_1], [mp_2], [mp_3], [mp_4], [mp_5], [mp_6], [mp_7], [mp_8], [mp_9], [mp_10], [mp_11], [mp_12], [mp_13], [mp_14], [mp_15], [mp_16],
							[mp_17], [mp_18], [mp_19]))mp),

		ap AS
				(SELECT TimeStamp, SUBSTRING(apIndex,4,6) AS apIndex , ap,CV FROM Averages
				UNPIVOT		(ap FOR apIndex IN
							([ap_0],[ap_1], [ap_2], [ap_3], [ap_4], [ap_5], [ap_6], [ap_7], [ap_8], [ap_9], [ap_10], [ap_11], [ap_12], [ap_13], [ap_14], [ap_15], [ap_16],
							[ap_17], [ap_18], [ap_19]))ap),


		csfcp AS
				(SELECT TimeStamp, SUBSTRING(csfcpIndex,7,9) AS csfcpIndex , csfcp,CV FROM Averages
				UNPIVOT		(csfcp FOR csfcpIndex IN
							([csfcp_0], [csfcp_1], [csfcp_2], [csfcp_3], [csfcp_4], [csfcp_5], [csfcp_6], [csfcp_7], [csfcp_8], [csfcp_9], [csfcp_10], [csfcp_11], [csfcp_12],
							[csfcp_13], [csfcp_14], [csfcp_15], [csfcp_16], [csfcp_17],[csfcp_18], [csfcp_19])) csfcp),

		ratio AS
				(SELECT TimeStamp, SUBSTRING(ratioIndex,7,9) AS ratioIndex , ratio,CV FROM Averages
				UNPIVOT		(ratio FOR ratioIndex IN
							([ratio_0], [ratio_1], [ratio_2], [ratio_3], [ratio_4], [ratio_5], [ratio_6], [ratio_7], [ratio_8], [ratio_9], [ratio_10], [ratio_11], [ratio_12], [ratio_13],
							[ratio_14], [ratio_15], [ratio_16], [ratio_17], [ratio_18], [ratio_19])) ratio),

		p20_p50_p80 AS 
				(SELECT TimeStamp, SUBSTRING(p20_p50_p80Index,13,15) AS p20_p50_p80Index , p20_p50_p80,CV FROM Averages
				UNPIVOT		(p20_p50_p80 FOR p20_p50_p80Index IN
							(p20_p50_p80_0, p20_p50_p80_1, p20_p50_p80_2))p20_p50_p80),

		raw_p20_p50_p80 AS 
				(SELECT TimeStamp, SUBSTRING(raw_p20_p50_p80Index,17,18) AS raw_p20_p50_p80Index ,raw_p20_p50_p80, CV FROM Averages
				UNPIVOT		(raw_p20_p50_p80 FOR raw_p20_p50_p80Index IN
							( [raw_p20_p50_p80_0], [raw_p20_p50_p80_1], [raw_p20_p50_p80_2])) raw_p20_p50_p80),

		beltevents AS
				(SELECT TimeStamp, SUBSTRING(belteventsIndex,11,13) AS belteventsIndex , beltevents,CV FROM Averages
				UNPIVOT		(beltevents FOR belteventsIndex IN
							( [beltevents_0], [beltevents_1], [beltevents_2], [beltevents_3], [beltevents_4], [beltevents_5], [beltevents_6], [beltevents_7]))beltevents),

		oversizeevents AS
				(SELECT TimeStamp, SUBSTRING(oversizeeventsIndex,16,18) AS oversizeeventsIndex , oversizeevents,CV  FROM Averages
				UNPIVOT		(oversizeevents FOR oversizeeventsIndex IN
							([oversizeevents_0], [oversizeevents_1], [oversizeevents_2],[oversizeevents_3]))oversizeevents),

		top_size AS
				(SELECT TimeStamp, SUBSTRING(top_sizeIndex,10,12) AS top_sizeIndex , top_size,CV FROM Averages
				UNPIVOT		(top_size FOR top_sizeIndex IN
							([top_size_0]))top_size),

		
		Analysers AS (
SELECT

       sieve.TimeStamp,CAST(sieve.TimeStamp AS DATE) AS Date,CAST(sieve.TimeStamp AS TIME) AS Time,sieveIndex AS [Index],sieveSize,smp,sfcp,sfp,mp,ap,csfcp,ratio,
	   ISNULL(p20_p50_p80,0) as p20_p50_p80,ISNULL(raw_p20_p50_p80,0) as raw_p20_p50_p80,ISNULL(beltevents,0) as beltevents,ISNULL(oversizeevents,0) as oversizeevents,
	   ISNULL(top_size,0) as top_size,
	   sieve.CV
	   

FROM Sieve

LEFT JOIN Smp ON smp.smpIndex = sieve.sieveIndex AND smp.TimeStamp = sieve.TimeStamp AND smp.CV = sieve.CV
LEFT JOIN sfcp ON sfcp.sfcpIndex = sieve.sieveIndex AND sfcp.TimeStamp = sieve.TimeStamp AND sfcp.CV=sieve.CV
LEFT JOIN sfp ON sfp.sfpIndex = sieve.sieveIndex AND sfp.TimeStamp = sieve.TimeStamp AND sfp.CV = sieve.CV
LEFT JOIN mp ON mp.mpIndex = sieve.sieveIndex AND mp.TimeStamp = sieve.TimeStamp AND mp.CV = sieve.CV
LEFT JOIN ap ON ap.apIndex = sieve.sieveIndex AND ap.TimeStamp = sieve.TimeStamp AND ap.CV = sieve.CV
LEFT JOIN csfcp ON csfcp.csfcpIndex = sieve.sieveIndex AND csfcp.TimeStamp = sieve.TimeStamp AND csfcp.CV = sieve.CV
LEFT JOIN ratio ON ratio.ratioIndex = sieve.sieveIndex AND ratio.TimeStamp = sieve.TimeStamp AND ratio.CV = sieve.CV
LEFT JOIN p20_p50_p80 ON p20_p50_p80.p20_p50_p80Index = sieve.sieveIndex AND p20_p50_p80.TimeStamp = sieve.TimeStamp AND p20_p50_p80.CV = sieve.CV
LEFT JOIN raw_p20_p50_p80 ON raw_p20_p50_p80.raw_p20_p50_p80Index = sieve.sieveIndex AND raw_p20_p50_p80.TimeStamp = sieve.TimeStamp AND raw_p20_p50_p80.CV = sieve.CV
LEFT JOIN beltevents ON beltevents.belteventsIndex = sieve.sieveIndex AND beltevents.TimeStamp = sieve.TimeStamp AND beltevents.CV = sieve.CV
LEFT JOIN oversizeevents ON oversizeevents.oversizeeventsIndex = sieve.sieveIndex AND oversizeevents.TimeStamp = sieve.TimeStamp AND oversizeevents.CV = sieve.CV
LEFT JOIN top_size ON top_size.top_sizeIndex = sieve.sieveIndex AND top_size.TimeStamp = sieve.TimeStamp AND top_size.CV = sieve.CV
)




MERGE [dbo].Pcs_PSDAnalysers_Hourly AS TARGET USING Analysers AS SOURCE
	ON TARGET.[Index] = SOURCE.[Index] and TARGET.[TimeStamp] = SOURCE.[TimeStamp] and TARGET.CV = SOURCE.CV
WHEN NOT MATCHED BY TARGET
	THEN INSERT
		([TimeStamp],[Date],[Time],[Index],[SieveSize],[smp],[sfcp],[mp],[ap],[sfp],[csfcp]
		,[ratio],[P20_P50_P80],[raw_P20_P50_P80],[BeltEvents],[OverSizeEvents],[CV],[top_size])
	VALUES
		(SOURCE.TimeStamp,SOURCE.[Date],SOURCE.[Time],SOURCE.[Index],SOURCE.[SieveSize],SOURCE.[smp],SOURCE.[sfcp],SOURCE.[mp],SOURCE.[ap],SOURCE.[sfp],SOURCE.[csfcp]
		,SOURCE.[ratio],SOURCE.[P20_P50_P80],SOURCE.[raw_P20_P50_P80],SOURCE.[BeltEvents],SOURCE.[OverSizeEvents],SOURCE.CV, SOURCE.[top_size])

WHEN MATCHED
	THEN UPDATE
		SET
			TARGET.[TimeStamp]								= SOURCE.[TimeStamp]
			,TARGET.[Date]									= SOURCE.[Date]
			,TARGET.[Time]									= SOURCE.[Time]
			,TARGET.[Index]									= SOURCE.[Index]
			,TARGET.[SieveSize]								= SOURCE.[SieveSize]
			,TARGET.[smp]									= SOURCE.[smp]
			,TARGET.[sfcp]									= SOURCE.[sfcp]
			,TARGET.[mp]									= SOURCE.[mp]
			,TARGET.[ap]									= SOURCE.[ap]
			,TARGET.[sfp]									= SOURCE.[sfp]	
			,TARGET.[csfcp]									= SOURCE.[csfcp]
			,TARGET.[ratio]									= SOURCE.[ratio]
			,TARGET.[P20_P50_P80]							= SOURCE.[P20_P50_P80]
			,TARGET.[raw_P20_P50_P80]						= SOURCE.[raw_P20_P50_P80]
			,TARGET.[BeltEvents]							= SOURCE.[BeltEvents]
			,TARGET.[OverSizeEvents]						= SOURCE.[OverSizeEvents]
			,TARGET.[CV]									= SOURCE.[CV]
			,TARGET.[top_size]								= SOURCE.[top_size]
;	"""

SQL_drop_temp = """DROP TABLE IF EXISTS #temp_Analysers"""

SQL_create_temp = """DROP TABLE IF EXISTS #temp_Analysers;
CREATE TABLE #temp_Analysers
(
    [data_id] [int] NOT NULL,
	[when_captured] [datetime] NULL,
	[all_ok_0] [float] NULL,
	[sievesize_0] [float] NULL,
	[sievesize_1] [float] NULL,
	[sievesize_2] [float] NULL,
	[sievesize_3] [float] NULL,
	[sievesize_4] [float] NULL,
	[sievesize_5] [float] NULL,
	[sievesize_6] [float] NULL,
	[sievesize_7] [float] NULL,
	[sievesize_8] [float] NULL,
	[sievesize_9] [float] NULL,
	[sievesize_10] [float] NULL,
	[sievesize_11] [float] NULL,
	[sievesize_12] [float] NULL,
	[sievesize_13] [float] NULL,
	[sievesize_14] [float] NULL,
	[sievesize_15] [float] NULL,
	[sievesize_16] [float] NULL,
	[sievesize_17] [float] NULL,
	[sievesize_18] [float] NULL,
	[sievesize_19] [float] NULL,
	[smp_0] [float] NULL,
	[smp_1] [float] NULL,
	[smp_2] [float] NULL,
	[smp_3] [float] NULL,
	[smp_4] [float] NULL,
	[smp_5] [float] NULL,
	[smp_6] [float] NULL,
	[smp_7] [float] NULL,
	[smp_8] [float] NULL,
	[smp_9] [float] NULL,
	[smp_10] [float] NULL,
	[smp_11] [float] NULL,
	[smp_12] [float] NULL,
	[smp_13] [float] NULL,
	[smp_14] [float] NULL,
	[smp_15] [float] NULL,
	[smp_16] [float] NULL,
	[smp_17] [float] NULL,
	[smp_18] [float] NULL,
	[smp_19] [float] NULL,
	[sfcp_0] [float] NULL,
	[sfcp_1] [float] NULL,
	[sfcp_2] [float] NULL,
	[sfcp_3] [float] NULL,
	[sfcp_4] [float] NULL,
	[sfcp_5] [float] NULL,
	[sfcp_6] [float] NULL,
	[sfcp_7] [float] NULL,
	[sfcp_8] [float] NULL,
	[sfcp_9] [float] NULL,
	[sfcp_10] [float] NULL,
	[sfcp_11] [float] NULL,
	[sfcp_12] [float] NULL,
	[sfcp_13] [float] NULL,
	[sfcp_14] [float] NULL,
	[sfcp_15] [float] NULL,
	[sfcp_16] [float] NULL,
	[sfcp_17] [float] NULL,
	[sfcp_18] [float] NULL,
	[sfcp_19] [float] NULL,
	[sfp_0] [float] NULL,
	[sfp_1] [float] NULL,
	[sfp_2] [float] NULL,
	[sfp_3] [float] NULL,
	[sfp_4] [float] NULL,
	[sfp_5] [float] NULL,
	[sfp_6] [float] NULL,
	[sfp_7] [float] NULL,
	[sfp_8] [float] NULL,
	[sfp_9] [float] NULL,
	[sfp_10] [float] NULL,
	[sfp_11] [float] NULL,
	[sfp_12] [float] NULL,
	[sfp_13] [float] NULL,
	[sfp_14] [float] NULL,
	[sfp_15] [float] NULL,
	[sfp_16] [float] NULL,
	[sfp_17] [float] NULL,
	[sfp_18] [float] NULL,
	[sfp_19] [float] NULL,
	[mp_0] [float] NULL,
	[mp_1] [float] NULL,
	[mp_2] [float] NULL,
	[mp_3] [float] NULL,
	[mp_4] [float] NULL,
	[mp_5] [float] NULL,
	[mp_6] [float] NULL,
	[mp_7] [float] NULL,
	[mp_8] [float] NULL,
	[mp_9] [float] NULL,
	[mp_10] [float] NULL,
	[mp_11] [float] NULL,
	[mp_12] [float] NULL,
	[mp_13] [float] NULL,
	[mp_14] [float] NULL,
	[mp_15] [float] NULL,
	[mp_16] [float] NULL,
	[mp_17] [float] NULL,
	[mp_18] [float] NULL,
	[mp_19] [float] NULL,
	[ap_0] [float] NULL,
	[ap_1] [float] NULL,
	[ap_2] [float] NULL,
	[ap_3] [float] NULL,
	[ap_4] [float] NULL,
	[ap_5] [float] NULL,
	[ap_6] [float] NULL,
	[ap_7] [float] NULL,
	[ap_8] [float] NULL,
	[ap_9] [float] NULL,
	[ap_10] [float] NULL,
	[ap_11] [float] NULL,
	[ap_12] [float] NULL,
	[ap_13] [float] NULL,
	[ap_14] [float] NULL,
	[ap_15] [float] NULL,
	[ap_16] [float] NULL,
	[ap_17] [float] NULL,
	[ap_18] [float] NULL,
	[ap_19] [float] NULL,
	[csfcp_0] [float] NULL,
	[csfcp_1] [float] NULL,
	[csfcp_2] [float] NULL,
	[csfcp_3] [float] NULL,
	[csfcp_4] [float] NULL,
	[csfcp_5] [float] NULL,
	[csfcp_6] [float] NULL,
	[csfcp_7] [float] NULL,
	[csfcp_8] [float] NULL,
	[csfcp_9] [float] NULL,
	[csfcp_10] [float] NULL,
	[csfcp_11] [float] NULL,
	[csfcp_12] [float] NULL,
	[csfcp_13] [float] NULL,
	[csfcp_14] [float] NULL,
	[csfcp_15] [float] NULL,
	[csfcp_16] [float] NULL,
	[csfcp_17] [float] NULL,
	[csfcp_18] [float] NULL,
	[csfcp_19] [float] NULL,
	[ratio_0] [float] NULL,
	[ratio_1] [float] NULL,
	[ratio_2] [float] NULL,
	[ratio_3] [float] NULL,
	[ratio_4] [float] NULL,
	[ratio_5] [float] NULL,
	[ratio_6] [float] NULL,
	[ratio_7] [float] NULL,
	[ratio_8] [float] NULL,
	[ratio_9] [float] NULL,
	[ratio_10] [float] NULL,
	[ratio_11] [float] NULL,
	[ratio_12] [float] NULL,
	[ratio_13] [float] NULL,
	[ratio_14] [float] NULL,
	[ratio_15] [float] NULL,
	[ratio_16] [float] NULL,
	[ratio_17] [float] NULL,
	[ratio_18] [float] NULL,
	[ratio_19] [float] NULL,
	[p20_p50_p80_0] [float] NULL,
	[p20_p50_p80_1] [float] NULL,
	[p20_p50_p80_2] [float] NULL,
	[raw_p20_p50_p80_0] [float] NULL,
	[raw_p20_p50_p80_1] [float] NULL,
	[raw_p20_p50_p80_2] [float] NULL,
	[top_size_0] [float] NULL,
	[smoothed_top_size_0] [float] NULL,
	[checksum_0] [float] NULL,
	[beltevents_0] [float] NULL,
	[beltevents_1] [float] NULL,
	[beltevents_2] [float] NULL,
	[beltevents_3] [float] NULL,
	[beltevents_4] [float] NULL,
	[beltevents_5] [float] NULL,
	[beltevents_6] [float] NULL,
	[imagehealth_0] [float] NULL,
	[imagehealth_1] [float] NULL,
	[imagehealth_2] [float] NULL,
	[imagehealth_3] [float] NULL,
	[imagehealth_4] [float] NULL,
	[imagehealth_5] [float] NULL,
	[imagehealth_6] [float] NULL,
	[external_0] [float] NULL,
	[oversizeevents_0] [float] NULL,
	[oversizeevents_1] [float] NULL,
	[oversizeevents_2] [float] NULL,
	[oversizeevents_3] [float] NULL,
	[beltevents_7] [float] NULL,
	[CV] [nvarchar](20) NULL
)"""

cursor2 = conn2.cursor()
cursor2.fast_executemany = True
try:
	cursor2.execute(SQL_create_temp)
	print('table created')

	InsertIntoSQL(dbvalues_list, sql)
	print('data inserted')

	print('Starting with the merge')
	cursor2.execute(merge)
	print('Data merged')

	cursor2.execute(SQL_drop_temp)
	conn2.commit()

	print('Yaaaaaayyyyy')


except Exception as e:
    print(e)
