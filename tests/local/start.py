
import boto3
from pydicom import filereader
import pydicom
from S3StreamingObj import S3StreamObj
import logging
from smart_open import open as so_open
import tarfile
import pandas as pd
import time,sys
from utils import serialize_sets, validateDCMKeyword, flatPN
S3_BUCKET = "hemande-dicom2"
S3_REGION = "us-west-2"
S3_KEY = "MammoTomoUPMC_Case14.tar.bz2"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

smartfplist = []
s3Streamfplist = []
localfplist = []


def recurse(ds, book={}, parent=""):
    for elm in ds:
        if elm.VR == 'SQ' and elm.keyword != "":
            [recurse(item, book=book,
                     parent=f"{validateDCMKeyword(elm)}.") for item in elm]
        elif elm.VR == 'UN':
            logger.warning(
                f"Skipped: Found Value Representation: UN for Tag {elm.tag} in DCM ; ")
        elif (elm.keyword == ""):
            logger.warning(f"Skipped: Found Empty Keyword Tag {elm.tag}")
        elif (elm.tag.is_private):
            logger.warning(f"Skipped: Found Private Tag {elm.tag}")
        elif elm.VR == 'PN':
            nestPN = flatPN(elm)
            for i in nestPN.keys():
                logger.debug(
                    f"TAG: {elm.tag}  Keyword: {parent}{elm.keyword}.{i} Value: {nestPN[i]}")
                book[parent + elm.keyword + "." + i] = nestPN[i]
        else:
            if parent == "":
                logger.debug(
                    f"TAG: {elm.tag}  Keyword: {elm.keyword} Value: {elm.value}")
                book[validateDCMKeyword(elm)] = serialize_sets(elm.value)
            else:
                logger.debug(
                    f"TAG: {elm.tag}  Keyword: {parent}.{elm.keyword} Value: {elm.value}")
                book[parent +
                     validateDCMKeyword(elm)] = serialize_sets(elm.value)


def smartgenerateBZ2List():
    logger.info("Starting Generate BZ2 Decompression")
    test = so_open(f"s3://{S3_BUCKET}/{S3_KEY}", mode="rb")
    t = tarfile.open(fileobj=test._fp, mode='r:bz2')
    listItems = t.getmembers()
    for item in listItems:
        if item.isfile():
            logger.info(f"Found File: {item.name}")
            bz = t.extractfile(item)
            bz.filename = item.name
            bz.archivefilename = item.name
            smartfplist.append(bz)
        else:
            logger.info(f"Skipped {item.name} : Not File")
    return ""

def localgenerateBZ2List():
    logger.info("Starting LOCAL Generate BZ2 Decompression")
    fileobj.download_file('/tmp/dicom.tar.bz')
    t = tarfile.open('/tmp/dicom.tar.bz', mode='r:bz2')
    listItems = t.getmembers()
    for item in listItems:
        if item.isfile():
            logger.info(f"Found File: {item.name}")
            bz = t.extractfile(item)
            bz.filename = item.name
            bz.archivefilename = item.name
            localfplist.append(bz)
        else:
            logger.info(f"Skipped {item.name} : Not File")
    return ""

def streamgenerateBZ2List():
    logger.info("Starting LOCAL Generate BZ2 Decompression")
    # fileobj.download_file('/tmp/dicom.tar.bz')
    filestreamobj = s3.Object(bucket_name=S3_BUCKET, key=S3_KEY)
    S3StreamObj(filestreamobj)
    t = tarfile.open(fileobj=S3StreamObj(filestreamobj), mode='r:bz2')
    listItems = t.getmembers()
    for item in listItems:
        if item.isfile():
            logger.info(f"Found File: {item.name}")
            bz = t.extractfile(item)
            bz.filename = item.name
            bz.archivefilename = item.name
            localfplist.append(bz)
        else:
            logger.info(f"Skipped {item.name} : Not File")
    return ""

def getDF(filelist):
    totalGets = 0
    totalBytes = 0
    dfList = []
    totalNonDCM = 0
    startTimer = time.perf_counter()
    for item in filelist:
        try:
            logger.info(f"Attempt to read File: {item.filename}")
            ds = filereader.read_partial(
                item, filereader._at_pixel_data, defer_size=None, force=False, specific_tags=None)

            ds.remove_private_tags()
            # # Build Python Dict
            FINAL_STRUCT = {}
            recurse(ds, book=FINAL_STRUCT)
            FINAL_STRUCT["S3Bucket"] = S3_BUCKET
            FINAL_STRUCT["S3Key"] = S3_KEY
            FINAL_STRUCT["S3BucketRegion"] = S3_REGION
            FINAL_STRUCT["S3KeyArchivePath"] = item.archivefilename
            df = pd.json_normalize(FINAL_STRUCT)
            logger.debug(
                f"Found {df.columns.values.size} Metadata items in Object")
            dfList.append(df)
            logger.info(
                f"Completed Read File: {item.filename} ; Generated {df.columns.values.size} columns")
        except pydicom.errors.InvalidDicomError as ee:
            totalNonDCM += 1
            if len(filelist) > 1:
                logger.warning(
                    f"Skipping non-DCM file: {item.filename} in archive")
                pass
            else:
                logger.error(ee)
                sys.exit(1)
    endTimer = time.perf_counter()

    if len(dfList) > 1:
        bigDf = pd.concat(dfList)
    else:
        bigDf = dfList[0]
    logger.info(
        f"Completed PUT s3://{S3_BUCKET}/{S3_KEY} to ")
    return bigDf


s3 = boto3.resource("s3")
s3 = boto3.resource("s3", region_name=S3_REGION)


fileobj = s3.Object(bucket_name=S3_BUCKET, key=S3_KEY)
streamgenerateBZ2List()
smartgenerateBZ2List()
localgenerateBZ2List()
smartdf = getDF(smartfplist)
localdf = getDF(localfplist)
streamdf = getDF(s3Streamfplist)
print(smartdf)
