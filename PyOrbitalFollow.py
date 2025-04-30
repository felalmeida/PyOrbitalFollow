#!/usr/bin/python3
# -*- coding: UTF-8 -*-
###############################################################################
# Module:   PyOrbitalFollow.py      Autor: Felipe Almeida                     #
# Start:    18-Jul-2024             LastUpdate: 16-Ago-2024     Version: 1.0  #
###############################################################################

import sys
import os
import time
import datetime
import requests
import json
import hashlib
import pytz
import skyfield.api # https://rhodesmill.org/skyfield/api.html#earth-satellites

ThisPath    = os.path.dirname(__file__)+'/'
ConfigPath  = ThisPath+'config/'
DataPath    = ThisPath+'data/'
TlePath     = ThisPath+'tle_files/'
FieldDelim  = ';'

if not os.path.exists(TlePath):  os.makedirs(TlePath)
if not os.path.exists(DataPath): os.makedirs(DataPath)

for root, dirs, files in os.walk(DataPath):
    for f in files:
        os.unlink(os.path.join(root, f))
    for d in dirs:
        shutil.rmtree(os.path.join(root, d))

with open(ConfigPath+'Locations.json', 'r') as fFileLocations:
    jLocations = json.load(fFileLocations)
jLocations = {key:val for key,val in jLocations.items() if val['Enabled'] == True}

with open(ConfigPath+'EarthStations.json', 'r') as fEarthStations:
    jEarthStations = json.load(fEarthStations)
jEarthStations = {key:val for key,val in jEarthStations.items() if val['Enabled'] == True}

jTLESources = []
with open(ConfigPath+'TLESources.json', 'r') as fTLESources:
    for TleSource in json.load(fTLESources):
        if (TleSource['Enabled'] == True):
            jTLESources.append(TleSource)

jTrackingSats = []
with open(ConfigPath+'TrackingSats.json', 'r') as fTrackingSources:
    for TrackingSat in json.load(fTrackingSources):
        if (TrackingSat['Enabled'] == True):
            jTrackingSats.append(TrackingSat)


def fixstr(v_Str):
    ReturnStr = v_Str
    ReturnStr = ReturnStr.strip().encode('utf-8', errors='ignore')
    ReturnStr = ReturnStr.decode('utf-8').replace(' ','_')
    ReturnStr = ReturnStr.replace('(','').replace(')','')
    return ReturnStr


def DictArrayToCsv(v_jArray,v_FieldDelim=','):
    CsvHeader = v_jArray[0].keys()
    CsvHeader = [Field for Field in CsvHeader if Field[0] != '_']
    CsvHeaderStr = v_FieldDelim.join(CsvHeader)+'\n'

    CsvBody = ''
    for jItem in v_jArray:
        CsvLine = ''
        for Field in CsvHeader:
            if Field in jItem.keys():
                CsvLine += str(jItem[Field]) + v_FieldDelim
        CsvLine = CsvLine[:-1]
        CsvLine += '\n'
        CsvBody += CsvLine

    return CsvHeaderStr+CsvBody


def DictArrayToLineJson(v_jArray):
    StrReturn = '['+'\n'
    for jItem in v_jArray:
        StrReturn += '    '+json.dumps(jItem,sort_keys=True)+',\n'
    StrReturn = StrReturn[:-2]+'\n'+']'+'\n'
    return StrReturn


def ParseJE9PELContent(v_WebContent):
    ContentArray = []
    jReturn = {}

    StatusDict = {
        '*': 'Active',
        'd': 'Deep space',
        'f': 'Failure',
        'i': 'Inactive',
        'n': 'Non-amateur',
        'r': 'Re-entered',
        't': 'To be launched',
        'u': 'Unknown',
        'w': 'Weather sat',
        '.': None
    }

    for Line in v_WebContent.split('\n'):
        StrLine = Line.strip()
        LineArray = []
        if (len(StrLine) == 0):
            continue
        if ('-----------------' in Line):
            continue
        if ('Callsign' in Line):
            continue

        Satellite   = '.' if len(StrLine[0:28].strip()) == 0 else StrLine[0:28].strip()
        SatId       = '.' if len(StrLine[28:36].strip()) == 0 else StrLine[28:36].strip()
        Status      = '.' if len(StrLine[124:].strip()) == 0 else StrLine[124:].strip()
        LineArray.append(Satellite)
        LineArray.append(SatId)

        '''
        Satellite   = StrLine[0:28]
        SatId       = StrLine[28:36]
        Uplink      = StrLine[36:54]
        Downlink    = StrLine[54:72]
        Beacon      = StrLine[72:84]
        Mode        = StrLine[84:111]
        Callsign    = StrLine[111:124]
        Status      = StrLine[124:]
        '''

        LineArray.extend(StrLine[36:124].split('  '))
        LineArray.append(Status)
        LineArray = [Item.strip() for Item in LineArray if len(Item.strip()) > 0]
        ContentArray.append(LineArray)

    for ArrayItem in ContentArray:
        Satellite   = None if ArrayItem[0] == '.' else ArrayItem[0]
        SatId       = None if ArrayItem[1] == '.' else ArrayItem[1]
        Uplink      = None if ArrayItem[2] == '.' else ArrayItem[2]
        Downlink    = None if ArrayItem[3] == '.' else ArrayItem[3]
        Beacon      = None if ArrayItem[4] == '.' else ArrayItem[4]
        Status      = ArrayItem[-1]

        Mode        = None
        Callsign    = None
        if (len(ArrayItem) == 8):
            Mode        = ArrayItem[5]
            Callsign    = ArrayItem[6]
        if (len(ArrayItem) == 7):
            Mode        = ArrayItem[5]

        jSatData = {
            'Satellite':    Satellite,
            'SatId':        SatId,
            'Uplink':       Uplink,
            'Downlink':     Downlink,
            'Beacon':       Beacon,
            'Mode':         Mode,
            'Callsign':     Callsign,
            'Status':       StatusDict[Status]
        }
        jReturn[str(Satellite)+'-'+str(SatId)] = jSatData

    return jReturn


def GetJE9PELWebsite():
    global TlePath

    JE9PELFileName  = os.path.realpath(TlePath+'JE9PEL.web')
    DownloadFile = True
    if os.path.exists(JE9PELFileName):
        FileStat = os.stat(JE9PELFileName)
        if (time.time() < FileStat.st_mtime + 86400):
            DownloadFile = False
            WebContent = open(JE9PELFileName).read()

    if (DownloadFile):
        print('Downloading JE9PEL Website Infos')
        WebURL = 'https://www.ne.jp/asahi/hamradio/je9pel/satslist.htm'
        WebPage = requests.get(WebURL)
        WebPage.encoding = 'utf-8'
        WebContent = WebPage.text
        with open(JE9PELFileName, 'w') as fWebFile:
            fWebFile.write(WebContent)

    # First Interact
    Bg = WebContent.find('Active (*)')
    WebContent = WebContent[Bg:]
    En = WebContent.find('#top')
    WebContent = WebContent[:En]

    # Second Interact
    Bg = WebContent.find('Satellite')
    WebContent = WebContent[Bg:]
    En = WebContent.find('</div>')
    WebContent = WebContent[:En]

    # Remove HTML Tags
    WebContent = WebContent.replace('<span style="background-color:#ccffcc;">','')
    WebContent = WebContent.replace('<span style="background-color:#dcdcdc;">','')
    WebContent = WebContent.replace('<span style="background-color:#c0c0c0;">','')
    WebContent = WebContent.replace('<span style="background-color:#dcdcdc">','')
    WebContent = WebContent.replace('</span>','')
    WebContent = WebContent.replace('<font color="#ff0000">','')
    WebContent = WebContent.replace('<font color="#ff4500">','')
    WebContent = WebContent.replace('<font color="#cc0000">','')
    WebContent = WebContent.replace('<font color="#aa0000">','')
    WebContent = WebContent.replace('<font color="#770000">','')
    WebContent = WebContent.replace('</font>','')
    WebContent = WebContent.replace('<a name="update"></a>','')
    WebContent = WebContent.replace('<a name="cas"></a>','')

    return WebContent


def ParseTLEs(v_TLEData):
    jTleData = {}
    SatId = None
    SatName = None
    ObjCount = 0
    for Line in v_TLEData.splitlines():
        TleLine = Line.strip()
        QtyFields = len(TleLine.split())
        if len(TleLine) == 0:
            continue
        if (QtyFields >= 8):
            jSatData['Line_'+TleLine[0].zfill(2)] = TleLine
        else:
            if (SatName is not None):
                jTleData[SatId] = jSatData.copy()
            ObjCount += 1
            SatId = TleLine+' '+str(ObjCount).zfill(5)
            SatName = TleLine
            jSatData = {}
            jSatData['Name'] = SatName
    jTleData[SatId] = jSatData.copy()
    return jTleData


def GetTLEs():
    global TlePath, jTLESources

    AllTLEData = []
    for TleSource in jTLESources:
        TleFileName  = os.path.realpath(TlePath+TleSource['Name'].replace(' ','_')+'.tle')
        TelSourceUrl = 'https://celestrak.org/NORAD/elements/gp.php?FORMAT=tle&GROUP='+str(TleSource['Group'])
        if (TleSource['Special']):
            TelSourceUrl = 'https://celestrak.org/NORAD/elements/gp.php?FORMAT=tle&SPECIAL='+str(TleSource['Group'])
        if (TleSource['Url'] is not None):
            TelSourceUrl = TleSource['Url']

        if TleSource['Enabled']:
            DownloadFile = True
            Error = False
            if os.path.exists(TleFileName):
                TleFileStat = os.stat(TleFileName)
                if (time.time() < TleFileStat.st_mtime + TleSource['TTL']):
                    DownloadFile = False
                    TleFileContent = open(TleFileName).read()
                    if (('404 - File Not Found' in TleFileContent) or ('Invalid query' in TleFileContent)):
                        os.remove(TleFileName)
                        Error = True
            if (DownloadFile):
                print('Downloading '+TleSource['Name']+' TLEs ( '+TelSourceUrl+' )')
                WebPage = requests.get(TelSourceUrl)
                WebPage.encoding = 'utf-8'
                TleFileContent = WebPage.text
                if (('404 - File Not Found' in TleFileContent) or ('Invalid query' in TleFileContent)):
                    print('===== Error in TLEs content for '+TleSource['Name']+' =====')
                    time.sleep(5)
                    Error = True
                else:
                    with open(TleFileName, 'w') as fTleFile:
                        fTleFile.write(TleFileContent)

            if (not Error):
                TleData = ParseTLEs(TleFileContent)
                AllTLEData.append({
                    'Name': TleSource['Name'],
                    'FileName': TleFileName,
                    'Url': TelSourceUrl,
                    'Source': TleSource,
                    'Objects': len(TleData),
                    'TLEs': TleData
                })

    return AllTLEData


def PrepareData(v_SaveFiles=False):
    global DataPath, jLocations, jEarthStations, jTrackingSats

    SaveFiles = v_SaveFiles

    IdsTracking = [jTracking['NORADCatalogNumber'] for jTracking in jTrackingSats]

    if (SaveFiles):
        FileStations    = os.path.realpath(DataPath+'satellites_stations.json')
        FileTLEs        = os.path.realpath(DataPath+'satellites_tles.json')
        FileJE9PEL      = os.path.realpath(DataPath+'satellites_je9pel.json')
        FileTLEsJE9PEL  = os.path.realpath(DataPath+'satellites_tles_je9pel.json')
        FileSatCatalog  = os.path.realpath(DataPath+'satellites_catalog.json')
        FileSatActive   = os.path.realpath(DataPath+'satellites_active.json')

    ### Stations and Locations
    SatStationsArray = []
    for StationIdx, StationValue in jEarthStations.items():
        ThisStation = StationValue.copy()
        if not ThisStation['Location'] in jLocations.keys():
            print('Invalid Location "'+ThisStation['Location']+'" for EarthStation "'+StationIdx+'"')
            continue
        else:
            ThisStation['LocationData'] = jLocations[ThisStation['Location']]
        SatStationsArray.append(ThisStation)
    if (SaveFiles):
        with open(FileStations,'w') as fFileStations:
            fFileStations.write(json.dumps(SatStationsArray,sort_keys=True,indent=4))

    ### Get TLEs From Sources
    SatTLEDocArray = []
    for SatTLE in GetTLEs():
        SatTLEDoc = SatTLE.copy()
        SatTLEDoc['_id'] = SatTLE['Name']
        SatTLEDoc['_insert_ts'] = int(datetime.datetime.now(datetime.UTC).timestamp())
        SatTLEDoc['_dt_insert'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
        SatTLEDocArray.append(SatTLEDoc)
    if (SaveFiles):
        with open(FileTLEs,'w') as fFileTLEs:
            fFileTLEs.write(json.dumps(SatTLEDocArray,sort_keys=True,indent=4))

    ### Get JE9PEL Data
    SatJE9PELArray = []
    for JE9PELIdx, JE9PELValue in ParseJE9PELContent(GetJE9PELWebsite()).items():
        SatJE9PELDoc = JE9PELValue.copy()
        SatJE9PELDoc['_id'] = SatJE9PELDoc['Satellite']
        SatJE9PELDoc['_insert_ts'] = int(datetime.datetime.now(datetime.UTC).timestamp())
        SatJE9PELDoc['_dt_insert'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
        SatJE9PELArray.append(SatJE9PELDoc)
    if (SaveFiles):
        with open(FileJE9PEL,'w') as fFileJE9PEL:
            fFileJE9PEL.write(json.dumps(SatJE9PELArray,sort_keys=True,indent=4))

    IdsFromJE9PEL = [JE9PELDoc['SatId'] for JE9PELDoc in SatJE9PELArray]

    ###########################################################################

    ### Generate Tracking Sats Catalog (Unique NORAD Catalog Number)
    TrackingSatsCatalog = []
    for SatTLEDoc in SatTLEDocArray:
        TleName = SatTLEDoc['Name']
        for SatId, SatData in SatTLEDoc['TLEs'].items():
            SatName  = SatData['Name']
            SatTle01 = SatData['Line_01']
            SatTle02 = SatData['Line_02']
            EarthSat = skyfield.api.EarthSatellite(SatTle01,SatTle02,SatName)
            SatNum   = EarthSat.model.satnum
            IntlDesg = EarthSat.model.intldesg
            HashStr  = SatTle01+SatTle02+SatName
            SatHash  = str(hashlib.md5((HashStr).encode('UTF-8')).hexdigest())
            jTleData = {
                'TleName':      TleName,
                'SatNumDesg':   str(SatNum)+'-'+str(IntlDesg),
                'SatId':        SatId,
                'SatName':      SatName,
                'SatNum':       SatNum,
                'SatHash':      SatHash,
                'SatData':      SatData,
                'IntlDesg':     IntlDesg,
                'HasJE9PEL':    (str(SatNum) in IdsFromJE9PEL),
                'Tracking':     (SatNum in IdsTracking)
            }
            jTleData['_id'] = jTleData['SatHash']
            jTleData['_insert_ts'] = int(datetime.datetime.now(datetime.UTC).timestamp())
            jTleData['_dt_insert'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
            TrackingSatsCatalog.append(jTleData)
    # Remove Duplicates
    TrackingSatsCatalog = list({v['_id']:v for v in TrackingSatsCatalog}.values())
    TrackingSatsCatalog = sorted(TrackingSatsCatalog, key=lambda DictItem:(DictItem['SatName']))
    if (SaveFiles):
        with open(FileSatCatalog,'w') as fFileSatCatalog:
            fFileSatCatalog.write(DictArrayToLineJson(TrackingSatsCatalog))

    ### Ajdust TLEs Data With JE9PEL Data
    SatTLEDocJE9PELArray = []
    for SatTLEDoc in TrackingSatsCatalog:
        SatTLEDocNew = SatTLEDoc.copy()
        if (SatTLEDocNew['HasJE9PEL']):
            JE9PELSat = [JE9PELDoc for JE9PELDoc in SatJE9PELArray if JE9PELDoc['SatId'] == str(SatTLEDocNew['SatNum'])][0]
            SatTLEDocNew['JE9PEL'] = JE9PELSat
        SatTLEDocJE9PELArray.append(SatTLEDocNew)
    if (SaveFiles):
        with open(FileTLEsJE9PEL,'w') as fFileTLEsJE9PEL:
            fFileTLEsJE9PEL.write(json.dumps(SatTLEDocJE9PELArray,sort_keys=True,indent=4))

    ### Map Active Satellites Data To Each Station
    SatMapConfigActiveArray = []
    for Station in SatStationsArray:
        ActiveSats = [jItem for jItem in jTrackingSats if jItem['EarthStation'] == Station['Id']]
        ActiveSatsIds = [jItem['NORADCatalogNumber'] for jItem in ActiveSats]
        SatStation = Station.copy()
        SatStation['Satellites'] = []
        for JE9PELSatellite in [jItem for jItem in SatTLEDocJE9PELArray if jItem['Tracking']]:
            SatSatellite = JE9PELSatellite.copy()
            if (SatSatellite['SatNum'] in ActiveSatsIds):
                SatSatellite['SatTrackingConfig'] = [jItem for jItem in ActiveSats if jItem['NORADCatalogNumber'] == SatSatellite['SatNum']][0]
                SatStation['Satellites'].append(SatSatellite)
        SatStation['_satellites_objects'] = len(SatStation['Satellites'])
        SatMapConfigActiveArray.append(SatStation)
    if (SaveFiles):
        with open(FileSatActive,'w') as fFileMapActive:
            fFileMapActive.write(json.dumps(SatMapConfigActiveArray,sort_keys=True,indent=4))
    # Sort Output
    SatMapConfigActiveArray = sorted(SatMapConfigActiveArray, key=lambda DictItem:(DictItem['Id']))

    ### Clear Variables
    del IdsTracking
    del SatStationsArray
    del SatTLEDocArray
    del SatJE9PELArray
    del IdsFromJE9PEL
    del TrackingSatsCatalog
    del SatTLEDocJE9PELArray

    return SatMapConfigActiveArray


def CalcPassages(v_SatelliteData=None, v_StationData=None, v_dtRefDateTime=datetime.datetime.now().date()):
    TleData         = v_SatelliteData['SatData']
    MiliSecStep     = v_SatelliteData['SatTrackingConfig']['TrackingStepMS']
    SecJumpStep     = v_SatelliteData['SatTrackingConfig']['WindowJumpSec']
    LocationData    = v_StationData['LocationData']

    dtStart         = v_dtRefDateTime
    dtTimeStart     = datetime.datetime(dtStart.year,dtStart.month,dtStart.day,0,0,0)
    dtTimeEnd       = datetime.datetime(dtStart.year,dtStart.month,dtStart.day,23,59,59) + datetime.timedelta(milliseconds=999)
    dtTimeScale     = skyfield.api.load.timescale()
    SatPOSDocArray  = []
    SatPOSMetadata  = []

    EarthSat        = skyfield.api.EarthSatellite(TleData['Line_01'],TleData['Line_02'],TleData['Name'],dtTimeScale)
    Location        = skyfield.api.wgs84.latlon(LocationData['Latitude'],LocationData['Longitude'],LocationData['Altitude'])
    TimeZone        = pytz.timezone(LocationData['TimeZone'])
    LocDiff         = EarthSat - Location
  # MaxIterations   = round((dtTimeEnd - dtTimeStart).total_seconds() * 1000 / MiliSecStep)
    print('Calculating For "'+LocationData['Name']+'" "'+v_SatelliteData['TleName']+'" "'+TleData['Name']+'"; Date '+v_dtRefDateTime.isoformat()+'; Step '+str(MiliSecStep)+'ms; MinDegree '+str(v_StationData['MinDegree']))

    dtTimeLoop      = dtTimeStart
    LoopStep        = datetime.timedelta(milliseconds=MiliSecStep)
    IsVisible       = False
    WindowVisible   = False
    MachineState    = None
    PassageSequence = 0
    WindowSequence  = 0
    WindowId        = 1
    WindowStart     = None
    WindowEnd       = None
    SatApexDegree   = 0
    SatApexTime     = None

    while dtTimeLoop <= dtTimeEnd:
        dtThisLoop      = TimeZone.localize(dtTimeLoop)
        tsStart         = dtTimeScale.utc(dtThisLoop)
        GeoCentric      = EarthSat.at(tsStart)
        TopoCentric     = LocDiff.at(tsStart)
        IsPassing       = False
        lat, lon        = skyfield.api.wgs84.latlon_of(GeoCentric)
        alt, az, dist   = TopoCentric.altaz()
        CalcDegress     = alt.degrees
        PassageSequence += 1
        WindowEnd       = None

        # State Machine
        if ((CalcDegress <= 0) and (not WindowVisible) and (not IsVisible)):
            MachineState = 'A'
            LoopStep = datetime.timedelta(minutes=5)
        elif ((CalcDegress > 0) and (not WindowVisible) and (not IsVisible)):
            MachineState    = 'B'
            WindowVisible   = True
            dtTimeLoop     -= datetime.timedelta(minutes=5)
            LoopStep        = datetime.timedelta(milliseconds=MiliSecStep)
        elif ((CalcDegress <= 0) and (WindowVisible) and (IsVisible)):
            if (MachineState == 'E'):
                WindowEnd       = dtThisLoop - datetime.timedelta(milliseconds=MiliSecStep)
                jPositionMetaData = {
                    'dtPassageDate':    v_dtRefDateTime.isoformat(),
                    'StationId':        LocationData['Id'],
                    'TleName':          v_SatelliteData['TleName'],
                    'SatName':          v_SatelliteData['SatName'],
                    'SatNum':           v_SatelliteData['SatNum'],
                    'TleHash':          v_SatelliteData['SatHash'],
                    'WindowId':         WindowId,
                    'WindowSteps':      WindowSequence,
                    'WindowStart':      WindowStart.isoformat(timespec='microseconds'),
                    'WindowEnd':        WindowEnd.isoformat(timespec='microseconds'),
                    'SatApexDegree':    SatApexDegree,
                    'SatApexTime':      SatApexTime.isoformat(timespec='microseconds')
                }
                SatPOSMetadata.append(jPositionMetaData)

                WindowSequence  = 0
                WindowVisible   = False
                WindowId       += 1
                SatApexDegree   = 0
                SatApexTime     = None
                dtTimeLoop     += datetime.timedelta(seconds=SecJumpStep)
                LoopStep        = datetime.timedelta(minutes=5)
            MachineState = 'C'
        elif ((CalcDegress <= 0) and (WindowVisible) and (not IsVisible)):
            MachineState = 'D'
        elif ((CalcDegress > 0) and (WindowVisible)):
            MachineState = 'E'
            WindowSequence += 1
            IsPassing = True
            if (WindowSequence == 1):
                WindowStart = dtThisLoop

        # Set Visibliity For Next Loop Iteration
        if (CalcDegress > 0):
            IsVisible = True
        else:
            IsVisible = False

        dtTimeLoop += LoopStep

        if (IsPassing):
            if (CalcDegress > SatApexDegree):
                SatApexDegree = CalcDegress
                SatApexTime = dtThisLoop

            jPositionData = {
                'PassageSequence':  PassageSequence,
                'WindowSequence':   WindowSequence,
                'WindowId':         WindowId,
                'DateTime':         dtThisLoop.isoformat(timespec='microseconds'),
                'Degress':          CalcDegress,
                'DistanceKm':       dist.km,
                'Azimuth':          az.degrees,
                'AzimuthArcSec':    az.arcseconds(),
                'Altitude':         alt.degrees,
                'AltitudeArcSec':   alt.arcseconds(),
                'Latitude':         lat.degrees,
                'LatitudeArcSec':   lat.arcseconds(),
                'Longitude':        lon.degrees,
                'LongitudeArcSec':  lon.arcseconds()
            }
            jPositionData['_id'] = str(v_SatelliteData['SatHash'])+'_'+str(PassageSequence).zfill(10)
            jPositionData['_insert_ts'] = int(datetime.datetime.now(datetime.UTC).timestamp())
            jPositionData['_dt_insert'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
            SatPOSDocArray.append(jPositionData)

    # Sort Output
    SatPOSDocArray = sorted(SatPOSDocArray, key=lambda DictItem:(DictItem['PassageSequence']))
    SatPOSMetadata = sorted(SatPOSMetadata, key=lambda DictItem:(DictItem['WindowId']))

    return [SatPOSMetadata, SatPOSDocArray]


def MainProcess():
    global DataPath, FieldDelim

    dtLoopStart = datetime.datetime.now().date()
    dtLoopEnd   = (dtLoopStart.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)
    dtLoopEnd   = dtLoopStart

    ### Calculate Satellite Position
    for Station in PrepareData(True):
        StLocation  = Station['LocationData']
        StMinDegree = Station['MinDegree']
        StFollows   = {}
        StConflicts = []

        if len(Station['Satellites']):
            FollowsBaseName   = DataPath+'FOL_'+dtLoopStart.strftime('%Y%m%d')+'_'+dtLoopEnd.strftime('%Y%m%d')+'_'+fixstr(Station['Name'])
            ConflictsBaseName = DataPath+'CON_'+dtLoopStart.strftime('%Y%m%d')+'_'+dtLoopEnd.strftime('%Y%m%d')+'_'+fixstr(Station['Name'])
            for StationSat in Station['Satellites']:
                TleData     = StationSat['SatData']
                JE9PELData  = StationSat['JE9PEL'] if StationSat['HasJE9PEL'] else None

                StFollows[StationSat['SatHash']] = {}
                dtLoopDate = dtLoopStart
                while dtLoopDate <= dtLoopEnd:
                    dtRefDateTime = dtLoopDate
                    dtStrDateTime = dtRefDateTime.strftime('%Y%m%d')

                    BaseName    = DataPath+'POS_'+dtStrDateTime+'_'+fixstr(Station['Name'])+'_'+fixstr(StationSat['SatName'])+'_'+StationSat['SatHash']
                    SatPassages = CalcPassages(v_SatelliteData=StationSat,v_StationData=Station,v_dtRefDateTime=dtRefDateTime)
                    jStationSatPassages = [jPassage for jPassage in SatPassages[1] if jPassage['Degress'] >= StMinDegree]
                    StFollows[StationSat['SatHash']][dtStrDateTime] = SatPassages[0]

                    if (StationSat['SatTrackingConfig']['Output_CSV']):
                        with open(os.path.realpath(BaseName+'.meta'),'w') as fCsvMetaFilePositions:
                            fCsvMetaFilePositions.write(DictArrayToCsv(SatPassages[0],FieldDelim))

                        with open(os.path.realpath(BaseName+'.csv'),'w') as fCsvFilePositions:
                            fCsvFilePositions.write(DictArrayToCsv(jStationSatPassages,FieldDelim))

                    if (StationSat['SatTrackingConfig']['Output_JSON']):
                        with open(os.path.realpath(BaseName+'.json'),'w') as fJsonFilePositions:
                            fJsonFilePositions.write(DictArrayToLineJson(jStationSatPassages))

                    dtLoopDate = dtLoopDate + datetime.timedelta(days=1)

            if (StationSat['SatTrackingConfig']['Output_JSON']):
                with open(os.path.realpath(FollowsBaseName+'.meta.json'),'w') as fJsonMetaFilePositions:
                    fJsonMetaFilePositions.write(json.dumps(StFollows,sort_keys=True,indent=4))

            ### Verify Station Passages Conflicts
            for TleHash, FollowData in StFollows.items(): # Each Station, TleId
                OthersStFollows = {key:val for key,val in StFollows.items() if key != TleHash}
                for dtPassageDate, StFollow in FollowData.items(): # Each TleId, Date
                    for OtherTleHash, OtherFollowData in OthersStFollows.items(): # Each Other Station, TleId
                        for OtherDtPassageDate, OtherStFollow in OtherFollowData.items(): # Each Other TleId, Date
                            if (dtPassageDate == OtherDtPassageDate): # Verify Only Same Date
                                for iPassage in StFollow: # Each Passage for TleId and Date
                                    iWindowStart = datetime.datetime.fromisoformat(iPassage['WindowStart'])
                                    iWindowEnd   = datetime.datetime.fromisoformat(iPassage['WindowEnd'])
                                    for jPassage in OtherStFollow: # Each Passage for Other TleId and Date
                                        jWindowStart = datetime.datetime.fromisoformat(jPassage['WindowStart'])
                                        jWindowEnd   = datetime.datetime.fromisoformat(jPassage['WindowEnd'])
                                        Conflict = False

                                        # Deslocated Windows
                                        if ((jWindowStart == iWindowStart) or (jWindowEnd == iWindowEnd)):
                                            Conflict = True
                                        elif (jWindowStart < iWindowStart and jWindowEnd < iWindowStart):   # Start and End Before
                                            Conflict = False
                                        elif (jWindowStart > iWindowEnd and jWindowEnd > iWindowEnd):       # Start and End After
                                            Conflict = False
                                        elif (jWindowStart > iWindowStart and jWindowEnd < iWindowEnd):     # Full Overlap
                                            Conflict = True
                                        elif (jWindowStart < iWindowStart and jWindowEnd > iWindowStart and jWindowEnd < iWindowEnd): # Start Overlap
                                            Conflict = True
                                        elif (jWindowStart > iWindowStart and jWindowStart < iWindowEnd and jWindowEnd > iWindowEnd): # End Overlap
                                            Conflict = True

                                        if (Conflict):
                                            StConflicts.append({
                                                'dtDate':               iWindowStart.date().isoformat(),
                                                'TleHash':              TleHash,
                                                'TleName':              iPassage['TleName'],
                                                'SatName':              iPassage['SatName'],
                                                'WindowId':             iPassage['WindowId'],
                                                'WindowStart':          iWindowStart.isoformat(timespec='microseconds'),
                                                'WindowEnd':            iWindowEnd.isoformat(timespec='microseconds'),
                                                'Conflict_TleHash':     OtherTleHash,
                                                'Conflict_TleName':     jPassage['TleName'],
                                                'Conflict_SatName':     jPassage['SatName'],
                                                'Conflict_WindowId':    jPassage['WindowId'],
                                                'Conflict_WindowStart': jWindowStart.isoformat(timespec='microseconds'),
                                                'Conflict_WindowEnd':   jWindowEnd.isoformat(timespec='microseconds')
                                            })

            if len(StConflicts):
                ### Remove Duplicate Conflicts
                StConflictsClean = []
                StConflicts.sort(key=lambda DictItem:(DictItem['dtDate'],DictItem['TleHash'],DictItem['WindowId'],DictItem['Conflict_TleHash']))
                for OriginalItem in StConflicts:
                    ItemExists = False
                    for VerifyItem in StConflictsClean:
                        if ((VerifyItem['dtDate'] == OriginalItem['dtDate']) and 
                            (VerifyItem['TleHash'] == OriginalItem['Conflict_TleHash']) and (VerifyItem['WindowId'] == OriginalItem['Conflict_WindowId']) and
                            (VerifyItem['Conflict_TleHash'] == OriginalItem['TleHash']) and (VerifyItem['Conflict_WindowId'] == OriginalItem['WindowId'])):
                            ItemExists = True
                            break
                    if (not ItemExists):
                        StConflictsClean.append(OriginalItem)
                StConflictsClean.sort(key=lambda DictItem:(DictItem['dtDate'],DictItem['TleHash'],DictItem['WindowId']))

                with open(os.path.realpath(ConflictsBaseName+'.csv'),'w') as fCsvConflicts:
                    fCsvConflicts.write(DictArrayToCsv(StConflictsClean,FieldDelim))

                if (StationSat['SatTrackingConfig']['Output_JSON']):
                    with open(os.path.realpath(ConflictsBaseName+'.json'),'w') as fJsonConflicts:
                        fJsonConflicts.write(json.dumps(StConflictsClean,sort_keys=True,indent=4))


def main():
    try:
        MainProcess()
        sys.exit(0)
    except KeyboardInterrupt:
        print("Py Orbital Follow Interrupted!")
        sys.exit(1)


if __name__ == "__main__":
    main()

