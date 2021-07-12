from array import array
import os,sys
import logging
import zipfile
import tarfile

logger = logging.getLogger('app')

class DcmList():

    DOWNLOAD_FILE_LIMIT_MB = int(os.environ.get("DOWNLOAD_FILE_LIMIT_MB",500))

    def __init__(self, s3Object):
        self.s3Object = s3Object
        self.key = s3Object.key
        self.bucket = s3Object.bucket_name
        self.fplist = []
        self.totalBytes = 0
        self.eventGets = 0
        self.fileext = os.environ.get("DEFAULT_FILE_EXTENSION","dcm")
        # self.S3streamObject = S3StreamObj(self.s3Object)
        self._setup()

    def _setup(self):
        try:
            self.fileext = self.key.split(".")[-1]
        except Exception as e:
            logger.warning(e)
            logger.warning(f"Unable to parse file ext from key: {self.key} , using default: {self.filext}")
            pass
    
    def generateFileList(self):
        logger.debug("DEBUGGING HERE SEE")
        if self.s3Object.content_length > (1000000 * self.DOWNLOAD_FILE_LIMIT_MB):
                raise RuntimeError("s3://{self.bucket}/{self.key} content length {self.s3Object.content_length} bytes greater than {self.DOWNLOAD_FILE_LIMIT_MB} MB")    
        if self.fileext == "dcm":
            logger.info(f"Received DCM extension type from s3://{self.bucket}/{self.key}")
            self.generateDCMList()
        elif self.fileext == "zip":
            logger.info(f"Received ZIP extension type from s3://{self.bucket}/{self.key}")
            self.generateZIPList()
        elif self.fileext == "gz":
            logger.info(f"Received GZ extension type from s3://{self.bucket}/{self.key}")
            self.generateZIPList()
        elif self.fileext == "bz2":
            logger.info(f"Received BZ2 extension type from s3://{self.bucket}/{self.key}")
            self.generateBZ2List()                        
        else:
            logger.warning(f"{self.fileext} extension type not recognized, attempting with {self.fileext}")

    def generateDCMList(self):
        logger.debug(f"Attempting to generate DCM File-Like Object")
        try:
                sf = self.downloadFile(self.key)
                sf.filename = sf.name
                sf.archivefilename = ""
                self.fplist.append(sf)
                # self.fplist.append(self.downloadFile(self.key))
        except Exception as e:
            # logger.error("Unable to generate S3 File-Like Object")
            logger.exception(e)
            raise RuntimeError("Unable to generate S3 File-Like Object")
    
    def generateZIPList(self):
        try:
            
            # Get Estimate of Uncompressed size
            # size = sum([zinfo.file_size for zinfo in zf.filelist])
            if self.s3Object.content_length > (1000000 * self.DOWNLOAD_FILE_LIMIT_MB):
                # self.generateZIPListStreaming()
                zf = zipfile.ZipFile(self.S3streamObject)
                fileList = zf.infolist()                
            else:
                fileobj = self.downloadFile(filename=self.key)
                zf = zipfile.ZipFile(fileobj)
                fileList = zf.infolist()               
            for item in fileList:
                if (item.filename.split("/")[-1].upper() == 'DICOMDIR'):
                    logger.warning(f"Skipped DICOMDIR file in Path: {item.filename}")
                elif item.file_size > 0:
                    sf = zf.open(item.filename)
                    sf.filename = sf.name
                    sf.archivefilename = sf.name
                    self.fplist.append(sf)
                    # self.fplist.append(zf.open(item.filename))
                    # self.fplist[-1]._fileobj._file.prefix = item.filename
                else:
                    logger.warning(f"Skipped Path: {item.filename} due to FileSize: {item.file_size} less than 1")                

        except Exception as e:
            logger.error(f"Unable to parse zip file s3://{self.bucket}/{self.key}")
            logger.exception(e)
            raise
    
    def generateBZ2List(self):
        logger.debug("Starting Generate BZ2 Decompression")
        fileobj = self.downloadFile(filename=self.key)
        t = tarfile.open(fileobj=fileobj, mode="r:bz2")
        listItems = t.getmembers()
        for item in listItems:
          if item.isfile():
              logger.info(f"Found File: {item.name}")
              bz = t.extractfile(item)
              bz.filename = item.name
              bz.archivefilename = item.name
              self.fplist.append(bz)
          else:
              logger.info(f"Skipped {item.name} : Not File")  
        return ""

    def generateTarList(self):
        logger.debug("Starting Generate BZ2 Decompression")
        fileobj = self.downloadFile(filename=self.key)
        t = tarfile.open(fileobj=fileobj, mode="r:bz2")
        listItems = t.getmembers()
        for item in listItems:
          if item.isfile():
              logger.info(f"Found File: {item.name}")
              bz = t.extractfile(item)
              bz.filename = item.name
              self.fplist.append(bz)
          else:
              logger.info(f"Skipped {item.name} : Not File")  
        return ""        
    def downloadFile(self,filename="default.dcm"):
        try:
            logger.info(f"S3 File Within {self.DOWNLOAD_FILE_LIMIT_MB} MB Attempting Download File")
            file = self.s3Object.download_file(f"/tmp/{filename}")
            # self.totalBytes += self.s3Object.content_length
            # self.eventGets +=1
            logger.info(f"Completed download s3://{self.bucket}/{self.key}")
            return open(f"/tmp/{filename}", mode='rb')
        except Exception as e:
            logger.error(f"Unable to Download s3://{self.bucket}/{self.key}")
            logger.exception(e)
            raise

    
    def getTotalGetRequests(self):
        return self.S3streamObject.eventGets + self.eventGets


    def getTotalBytesDownload(self):
        return self.S3streamObject.totalbytes + self.totalBytes    
    