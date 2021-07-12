
import json
import logging
import os
from os import getenv
import sys

import boto3

import pandas as pd
import boto3
from pydicom import filereader, dcmread, filebase
import pydicom
from utils import serialize_sets, validateDCMKeyword, flatPN, deleteSQSMsg
import awswrangler as wr
import time
from Dcm import DcmList

logger = logging.getLogger('app')
logger.setLevel(logging.INFO)


DEST_BUCKET: str = os.environ.get("DEST_BUCKET", "")
DEST_BUCKET_REGION: str = os.environ.get("DEST_BUCKET_REGION", "")
LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL","INFO").upper()

PYDICOM_DEBUG: str = os.environ.get('PYDICOM_DEBUG', 'FALSE')


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



def lambda_handler(event, context,nest=True):
    # pydicom.config.debug(debug_on=True)

    # Read JSON event from SQS Message Body
    if nest:

        data = json.loads(event["Records"][0]['body'])
        EVENT_BUCKET = data['Records'][0]['s3']['bucket']['name']
        EVENT_KEY = data['Records'][0]['s3']['object']['key']
        EVENT_REGION = data['Records'][0]['s3']['object']['awsRegion']
        EVENT_SQS_URL = event['Records'][0]['eventSourceARN']
        EVENT_SQS_MESSAGE_ID = event['Records'][0]['messageId']
        EVENT_SQS_RECIPTHANDLE = event['Records'][0]['receiptHandle']
    else:
        EVENT_BUCKET = event['S3_BUCKET']
        EVENT_KEY = event['S3_KEY']
        EVENT_REGION = event['S3_BUCKET_REGION']
        EVENT_SQS_URL = event['AWS_SQS_SOURCEARN']
        EVENT_SQS_MESSAGE_ID = event['AWS_SQS_MESSAGE_ID']
        EVENT_SQS_RECIPTHANDLE = event['AWS_SQS_RECIPTHANDLE']        
    logger.info(
        f"Received S3 Event for {EVENT_REGION} s3://{EVENT_BUCKET}/{EVENT_KEY}")
    try:
        s3 = boto3.resource("s3", region_name=EVENT_REGION)
        fileobj = s3.Object(bucket_name=EVENT_BUCKET, key=EVENT_KEY)

        if PYDICOM_DEBUG == 'TRUE':
            pydicom.config.debug(debug_on=True)
        
        fpList = DcmList(fileobj)
        fpList.generateFileList()
        dfList = []
        totalNonDCM = 0
        startTimer = time.perf_counter()
        for item in fpList.fplist:
            try:
                logger.info(f"Attempt to read File: {item.filename}")
                ds = filereader.read_partial(
                    item, filereader._at_pixel_data, defer_size=None, force=False, specific_tags=None)
                
                ds.remove_private_tags()
                # # Build Python Dict
                FINAL_STRUCT = {}
                recurse(ds, book=FINAL_STRUCT)
                FINAL_STRUCT["S3Bucket"] = EVENT_BUCKET
                FINAL_STRUCT["S3Key"] = EVENT_KEY
                FINAL_STRUCT["S3BucketRegion"] = EVENT_REGION
                FINAL_STRUCT["S3KeyArchivePath"] = item.archivefilename
                df = pd.json_normalize(FINAL_STRUCT)
                logger.debug(
                    f"Found {df.columns.values.size} Metadata items in Object")
                dfList.append(df)
                logger.info(f"Completed Read File: {item.filename} ; Generated {df.columns.values.size} columns")
            except pydicom.errors.InvalidDicomError as ee:
                totalNonDCM += 1
                if len(fpList.fplist) > 1:
                    logger.warning(
                        f"Skipping non-DCM file: {item.filename} in archive")
                    pass
                else:
                    logger.error(ee)
                    raise
        if len(dfList) > 1:
                bigDf = pd.concat(dfList)
        else:
                bigDf = dfList[0]
        logger.info(
            f"Attempt to convert to Parquet and out to S3 Bucket {DEST_BUCKET}")
        session = boto3.session.Session(region_name=DEST_BUCKET_REGION)
        s3Write = wr.s3.to_parquet(
            df=bigDf,
            path=f"s3://{DEST_BUCKET}/",
            dataset=True,
            compression="snappy",
            sanitize_columns=True,
            boto3_session=session,
        )
        logger.info(
            f"Completed PUT s3://{EVENT_BUCKET}/{EVENT_KEY} to {s3Write['paths']}")
        deleteSQSMsg(QUrl=EVENT_SQS_URL,RHandle=EVENT_SQS_RECIPTHANDLE,messageid=EVENT_SQS_MESSAGE_ID)
    except Exception as e:
        logger.error("Unable to get Process S3 Object")
        logger.exception(e)
        raise


if __name__ == "__main__":
    eventsFolder = os.path.dirname(os.path.abspath(__file__))
    data = {}
   
    data['S3_BUCKET'] = os.getenv("AWS_S3_BUCKET","")
    data['S3_KEY'] = os.getenv("AWS_S3_KEY","")
    data['S3_BUCKET_REGION'] = os.getenv("AWS_S3_BUCKET_REGION","")
    data['AWS_SQS_RECIPTHANDLE'] = os.getenv("AWS_SQS_RECIPTHANDLE","")
    data['AWS_SQS_MESSAGE_ID'] = os.getenv("AWS_SQS_MESSAGE_ID","")
    data['AWS_SQS_SOURCEARN'] = os.getenv("AWS_SQS_SOURCEARN","")
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger = logging.getLogger('app')
    logger.setLevel(LOGGING_LEVEL)
    # logging.getLogger('botocore').setLevel(logging.INFO)
    # boto3.set_stream_logger('',logging.CRITICAL)
    lambda_handler(data, None,nest=False)
