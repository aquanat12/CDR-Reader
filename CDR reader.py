import csv
import time
import glob
import shutil, os
import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="cdr"
)

mycursor = mydb.cursor()

def function():
    CDRfile = glob.glob(r"SFTP Path\cdr_StandAloneCluster_01_*")
    CMRfile = glob.glob(r"SFTP Path\cmr_StandAloneCluster_01_*")
    for textfile in CDRfile:
        with open(textfile) as csvfile:
            data = pd.read_csv(csvfile, skiprows = [1])
            #columns= ['globalCallID_callManagerId','globalCallID_callId','dateTimeOrigination','callingPartyNumber','originalCalledPartyNumber','finalCalledPartyNumber','dateTimeConnect','dateTimeDisconnect','lastRedirectDn','pkid','originalCalledPartyNumberPartition','callingPartyNumberPartition','finalCalledPartyNumberPartition','lastRedirectDnPartition','duration','origDeviceName','destDeviceName','origIpv4v6Addr','destIpv4v6Addr','originalCalledPartyPattern','finalCalledPartyPattern','huntPilotPattern','origDeviceType','destDeviceType']
            df = pd.DataFrame(data)
            df = df.fillna('')
            for index, row in df.iterrows():
                sql = "INSERT INTO cdr (globalCallID_callManagerId, globalCallID_callId, dateTimeOrigination, callingPartyNumber, originalCalledPartyNumber, finalCalledPartyNumber, dateTimeConnect, dateTimeDisconnect, lastRedirectDn, pkid, wasCallQueued, totalWaitTimeInQueue, originalCalledPartyNumberPartition, callingPartyNumberPartition, finalCalledPartyNumberPartition, lastRedirectDnPartition, duration, origDeviceName, destDeviceName, origIpv4v6Addr, destIpv4v6Addr, originalCalledPartyPattern, finalCalledPartyPattern, lastRedirectingPartyPattern, huntPilotPattern) VALUES (%s, %s, %s, %s ,%s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s ,%s, %s, %s)"
                val = (row['globalCallID_callManagerId'], row['globalCallID_callId'], row['dateTimeOrigination'], row['callingPartyNumber'], row['originalCalledPartyNumber'], row['finalCalledPartyNumber'], row['dateTimeConnect'], row['dateTimeDisconnect'], row['lastRedirectDn'], row['pkid'], row['wasCallQueued'], row['totalWaitTimeInQueue'], row['originalCalledPartyNumberPartition'], row['callingPartyNumberPartition'], row['finalCalledPartyNumberPartition'], row['lastRedirectDnPartition'], row['duration'], row['origDeviceName'], row['destDeviceName'], row['origIpv4v6Addr'], row['destIpv4v6Addr'], row['originalCalledPartyPattern'], row['finalCalledPartyPattern'], row['lastRedirectingPartyPattern'], row['huntPilotPattern'])
                print (val)
                mycursor.execute(sql, val)
                mydb.commit()
        shutil.move(textfile, r"SFTP Path\CDRBak")
    for cmrfiles in CMRfile:
        shutil.move(cmrfiles, r"SFTP Path\CMRBak")
while True:
    function()
    time.sleep(60)